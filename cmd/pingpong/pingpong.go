package main

/*

To ensure profit, sells are always +1 sat baseline. At lower levels it does a lot for fees. Buys might not adjust anything?

Listen via redis for pubsub events on orders completing or partially filling

*/

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/grammaton76/cryptohandlers/pkg/okane"
	_ "github.com/grammaton76/g76golib/chatoutput/sc_dbtable"
	"github.com/grammaton76/g76golib/shared"
	"github.com/grammaton76/g76golib/slogger"
	"strings"
	"sync"
	"time"
)

var log slogger.Logger

const STRATEGY string = "pingpong"

var Config shared.Configuration

var Cli struct {
	Inifile  string
	ReadOnly bool
	Debug    bool
}

var Chatlog *shared.ChatTarget

var Global struct {
	db                  *shared.DbHandle
	RateDelay           time.Duration
	LocalStorage        string
	MarketDataFile      string
	MarketRatesFile     string
	MarketIntervalsFile string
	PidFile             string
	Markets             *okane.MarketDefsType
	MarketDataCycle     int
	PullInterval        *shared.Stmt
	ReadOnly            bool
	Users               []*okane.User
	UserNames           []string
	Mux                 shared.SafeIntCache
	Orderlist           chan *okane.ActionRequest
	ChatSection         string
	ChatHandle          *shared.ChatHandle
	MsgStack            []string
	xMsgStack           sync.Mutex
	Strats              map[int]*strat_pingpong
}

type strat_pingpong struct {
	Id            int
	xTune         sync.Mutex
	Market        *okane.MarketDef
	Account       *okane.Account
	Pingpong      *okane.Stairway
	ReinvestRatio float64
	RestockRatio  float64
	BuyLimit      float64
	BuyQty        float64
	SellLimit     float64
	SellQty       float64
	LastUuid      string
	Action        chan *okane.Order
	Mode          string
	State         string
	RequestId     int
	Active        bool
	Suspended     bool
}

func ChatQueueThread() {
	for true {
		if len(Global.MsgStack) == 0 {
			time.Sleep(time.Duration(10) * time.Second)
			continue
		}
		Global.xMsgStack.Lock()
		var Text string
		for _, v := range Global.MsgStack {
			Text += fmt.Sprintf("%s\n", v)
		}
		Global.MsgStack = nil
		Global.xMsgStack.Unlock()
		Chatlog.Sendf("%s", Text)
		time.Sleep(time.Second)
	}
}

func ChatBuffer(format string, options ...interface{}) {
	log.Printf("Sending to chatBuffer...\n")
	Text := fmt.Sprintf(format, options...)
	Global.xMsgStack.Lock()
	Global.MsgStack = append(Global.MsgStack, Text)
	Global.xMsgStack.Unlock()
	log.Printf("Adding to chat buffer: %s\n", Text)
}

func LoadConfigValues(Ini string) {
	log.Init()
	slogger.SetLogger(&log)
	shared.SetLogger(&log)
	okane.SetLogger(&log)
	Global.Mux = shared.NewSafeIntCache()
	OtherIni := flag.String("inifile", "", "Specify an INI file for settings")
	ReadOnly := flag.Bool("readonly", false, "No writes to database or filesystem.")
	Debug := flag.Bool("debug", false, "Enable verbose debugging.")
	flag.Parse()
	Cli.Inifile = *OtherIni
	Cli.ReadOnly = *ReadOnly
	Cli.Debug = *Debug
	if Cli.Debug {
		log.SetThreshold(slogger.DEBUG)
	}
	Global.ReadOnly = Cli.ReadOnly
	if Cli.Inifile != "" {
		Ini = Cli.Inifile
	}
	Config.LoadAnIni(Ini).OrDie("Couldn't load INI")
	Global.PidFile = Config.GetStringOrDefault("pingpong.pidfile", "/data/baytor/state/pingpong.pid", "Defaulting pid file")
	shared.ExitIfPidActive(Global.PidFile)
	found, List := Config.GetString("okane.userlist")
	if found {
		Global.UserNames = strings.Split(List, ",")
	}
	Global.ChatSection = Config.GetStringOrDefault("pingpong.chathandle", "", "No chat channel defined; no notices will be sent!\n")
	Global.RateDelay = time.Second / 10
}

func InitAndConnect() {
	Global.db = Config.ConnectDbKey("okane.centraldb").OrDie()
	okane.DbInit(Global.db)
	Global.PullInterval = Global.db.PrepareOrDie("select marketid,max(last),min(last),max(lastusd),min(lastusd) from marketlast where exchangeid=1 and timeat>$1 group by marketid;")
	for _, Section := range Global.UserNames {
		Username := Config.GetStringOrDie(Section+".username", "Username is required")
		log.Printf("Init'ing user '%s'\n", Section)
		UserDb := Config.ConnectDbKey(Section + ".database").OrDie()
		if UserDb.IsDead() {
			log.Printf("Cannot connect to db for '%s', skipping this user.\n", Section)
			continue
		}
		User := okane.NewUser(UserDb)
		User.Username = Username
		Global.Users = append(Global.Users, User)
	}
	if Global.ChatSection != "" {
		Global.ChatHandle = Config.NewChatHandle(Global.ChatSection).OrDie("failed to define chat handle")
		Chatlog = Global.ChatHandle.OutputChannel
	}
	if Chatlog == nil {
		log.Fatalf("Sorry, we have to be able to send chats. Define the output channel.\n")
	}
	Global.Orderlist = make(chan *okane.ActionRequest)
	Global.Strats = make(map[int]*strat_pingpong)
}

func (PiP *strat_pingpong) Identifier() string {
	if PiP == nil {
		return "nil pingpong"
	}
	var Market string
	if PiP.Market == nil {
		Market = "undef_marketname"
	} else {
		Market = PiP.Market.Name()
	}
	return fmt.Sprintf("%s:%s:%d", PiP.Account.Identifier(), Market, PiP.Id)
}

func TestInteractive(PiP *strat_pingpong) {
	log.Printf("Entering interactive test for market '%s'.\n", PiP.Market.Identifier())
	for true {
		fmt.Printf("Enter command [%s]: ", PiP.Identifier())
		var input string
		fmt.Scanln(&input)
		log.Printf("Received '%s'\n", input)
		switch input {
		case "last":
			Quote := PiP.Market.GetLastRateDb()
			log.Printf("Current market rate: %s\n", Quote.Last.String())
		}
	}
}

func PingpongConfigThread(Acct *okane.Account) {
	var StartupDone bool
	var GetStrats *shared.Stmt
	var CacheCycles int
	if Acct.X["sth_getstrats"] == nil {
		GetStrats = Acct.User.Db.PrepareOrDie(
			"SELECT id,market,reinvest_ratio,restock_ratio,buylimit,buyqty,selllimit,sellqty,lastuuid,restock_ratio,active,suspended,mode,state,requestid FROM strat_pingpong WHERE name=$1 AND exchangeid=$2;")
		Acct.X["sth_getstrats"] = GetStrats
	} else {
		GetStrats = Acct.X["sth_getstrats"].(*shared.Stmt)
	}
	Exchange := Acct.Exchange
	for true {
		Strats, err := GetStrats.Query(STRATEGY, Acct.Exchange.Id)
		log.ErrorIff(err, "Failed to get stratdata for %s\n", Acct.Identifier())
		for Strats.Next() {
			if !StartupDone {
				DefineStatements(Acct)
				Exchange.GetMarketsDb(time.Hour * 12)
				log.Debugf("Finished getting markets from db for %s.\n", Exchange.Name)
				Exchange.GetMarketRatesDb()
				log.Debugf("Finished getting market rates from db for %s.\n", Exchange.Name)
				go Acct.RunOrderCache(time.Duration(10)*time.Second, &CacheCycles)
				for CacheCycles == 0 {
					time.Sleep(time.Second)
				}
				StartupDone = true
			}
			var S = &strat_pingpong{
				Account: Acct,
			}
			var marketname string
			var requestid sql.NullInt64
			var LastUuid sql.NullString
			err = Strats.Scan(&S.Id, &marketname, &S.ReinvestRatio, &S.RestockRatio, &S.BuyLimit, &S.BuyQty, &S.SellLimit, &S.SellQty, &LastUuid, &S.RestockRatio, &S.Active, &S.Suspended, &S.Mode, &S.State, &requestid)
			log.FatalIff(err, "Error on strategy scan: %s\n", err)
			if requestid.Valid {
				S.RequestId = int(requestid.Int64)
			}
			if LastUuid.Valid {
				S.LastUuid = LastUuid.String
			}
			found, Market := Exchange.Markets.ByName(marketname)
			if !found {
				log.Errorf("Pingpong %s: Couldn't find market '%s' on %s; skipping pingpong thread spawn.\n",
					S.Identifier(), marketname, Exchange.Identifier())
				continue
			} else {
				S.Market = Market
			}
			// TODO: This needs to be per account, or we have id collisions due to multiple strat_pingpong tables.
			E := Global.Strats[S.Id]
			if E == nil {
				log.Printf("Launching newly added pingpong %s\n", S.Identifier())
				Global.Strats[S.Id] = S
				go S.Launch()
			} else {
				E.xTune.Lock()
				if E.Suspended != S.Suspended {
					E.Suspended = S.Suspended
				}
				if E.Active != S.Active {
					E.Active = S.Active
				}
				if E.ReinvestRatio != S.ReinvestRatio {
					E.ReinvestRatio = S.ReinvestRatio
				}
				if E.RestockRatio != S.RestockRatio {
					E.RestockRatio = S.RestockRatio
				}
				if E.Mode != S.Mode {
					E.Mode = S.Mode
				}
				E.xTune.Unlock()
			}
		}
		time.Sleep(time.Minute)
	}
}

func DefineStatements(Acct *okane.Account) {
	// Select from stratdata and find pingpongs for this user.
	Exchange := Acct.Exchange

	log.Printf("Defining db statements for '%s' on %s\n",
		Acct.Identifier(), Exchange.Identifier())

	if Acct.X["sth_checkorderopen"] == nil {
		Acct.X["sth_checkorderopen"] = Acct.User.Db.PrepareOrDie(
			"SELECT type,bidlimit,quantity,filled,opened,closed FROM o_order_open WHERE uuid=$1 AND market=$2;")
	}
	if Acct.X["sth_pingpong_updatetargets"] == nil {
		Acct.X["sth_pingpong_updatetargets"] = Acct.User.Db.PrepareOrDie(
			"UPDATE strat_pingpong SET mode=$2, buyqty=$3, sellqty=$4 WHERE id=$1;")
	}
	if Acct.X["sth_pingpong_setsuspended"] == nil {
		Acct.X["sth_pingpong_setsuspended"] = Acct.User.Db.PrepareOrDie(
			"UPDATE strat_pingpong SET suspended=$2 WHERE id=$1;")
	}
	if Acct.X["sth_pingpong_updateorders"] == nil {
		Acct.X["sth_pingpong_updateorders"] = Acct.User.Db.PrepareOrDie(
			"UPDATE strat_pingpong SET lastuuid=$2,requestid=$3 WHERE id=$1;")
	}
	if Acct.X["sth_addstratorder"] == nil {
		Acct.X["sth_addstratorder"] = Acct.User.Db.PrepareOrDie(
			"INSERT INTO stratorders (uuid, stratowner, market, type, quantity, price, active) VALUES ($1, $2, $3, $4, $5, $6, true);")
	}
	if Acct.X["sth_checkarchive"] == nil {
		Acct.X["sth_checkarchive"] = Acct.User.Db.PrepareOrDie(
			"SELECT type,bidlimit,quantity,filled,opened,closed FROM o_order_archive WHERE uuid=$1 AND market=$2;")
	}
}

func (PiP *strat_pingpong) AssertOrder() (*okane.ActionRequest, error) {
	var Qty float64
	var Limit float64
	var Order string
	var Label string
	var Letter string
	switch PiP.Mode {
	case "SELL":
		Qty = PiP.SellQty
		Limit = PiP.SellLimit
		Order = "LIMIT_SELL"
		Letter = "S"
	case "BUY":
		Qty = PiP.BuyQty
		Limit = PiP.BuyLimit
		Order = "LIMIT_BUY"
		Letter = "B"
	default:
		log.Fatalf("Unknown order mode '%s' on pingpong '%s'\n",
			PiP.Mode, PiP.Identifier())
	}
	Label = PiP.Identifier() + "/" + Letter
	O := &okane.ActionRequest{
		Account:  PiP.Account,
		Command:  Order,
		Market:   PiP.Market,
		Qty:      Qty,
		Bidlimit: Limit,
		Stratid:  PiP.Id,
		Label:    Label,
		Subscribe: okane.ActionRequestSub{
			Result: make(chan *okane.ActionRequest),
			Id:     make(chan int),
		},
	}
	// Stage 1: We forward to requesting thread; we have no uuid or requestid.
	log.Printf("%s Relaying to OrderList channel.\n", PiP.Identifier())
	Global.Orderlist <- O
	if O.Error != nil {
		return nil, O.Error
	}
	log.Printf("%s Listening for result of subscription.\n", PiP.Identifier())
	Id := <-O.Subscribe.Id
	// Stage 2a: We have requestid now, but no uuid yet.
	PiP.RequestId = Id
	UpdateUuids := PiP.Account.X["sth_pingpong_updateorders"].(*shared.Stmt)
	var err error
	// Stage 2b: We write the requestid to the table.
	_, err = UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
	log.FatalIff(err, "Pingpong %s: id %d received; failed to update uuid '%s' / requestid %d",
		PiP.Identifier(), Id, PiP.LastUuid, PiP.RequestId)
	// Stage 3: We have a UUID now, and we wipe the requestid out.
	Result := <-O.Subscribe.Result
	log.Printf("%s Received result of subscription.\n", PiP.Identifier())
	PiP.LastUuid = Result.Uuid
	PiP.RequestId = 0
	_, err = UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
	log.FatalIff(err, "Pingpong %s: uuid '%s' received; failed to update uuid '%s' / requestid %d",
		PiP.Identifier(), Result.Uuid, PiP.LastUuid, PiP.RequestId)
	return Result, Result.Error
}

func OrderRequestThread() {
	log.Printf("Started order request thread.\n")
	for true {
		r := <-Global.Orderlist
		log.Printf("Received request in OrderList channel.\n")
		err := r.Account.SendRequest(r)
		if err != nil {
			r.Error = err
			log.Errorf("sending request %s: %s\n", r.Identifier(), err)
			if r.Subscribe.Result != nil {
				r.Subscribe.Result <- r
			}
		} else {
			log.Printf("Request %s assigned id %d\n", r.Identifier(), r.Id)
		}
	}
	log.Fatalf("Exited OrderRequest thread.\n")
}

func (PiP *strat_pingpong) Launch() {
	var err error
	log.Printf("Running launch() for %s\n", PiP.Identifier())
	A := PiP.Account
	RecordFlipTargets := A.X["sth_pingpong_updatetargets"].(*shared.Stmt)
	UpdateUuids := A.X["sth_pingpong_updateorders"].(*shared.Stmt)
	for {
		if !PiP.Active || PiP.Suspended {
			log.Debugf("Pingpong %s suspended/inactive; sleeping.\n",
				PiP.Identifier())
			time.Sleep(time.Minute)
			continue
		}
		LastUuid := PiP.LastUuid
		Market := PiP.Market.Name()
		var Requested *okane.ActionRequest
		if PiP.RequestId != 0 {
			log.Printf("Pingpong %s has request id %d; checking status.\n", PiP.Identifier(), PiP.RequestId)
			Requested = A.Cache.PendingIds[PiP.RequestId]
			if Requested != nil {
				switch Requested.Status {
				case "REJECTED":
					log.Printf("Pingpong %s: Suspending activity due to rejected request %d.\n",
						PiP.Identifier(), Requested.Id)
					PiP.Suspend()
					Requested = nil
				case "REQUESTED":
					log.Printf("Pingpong %s: awaiting result of request %d.\n",
						PiP.Identifier(), PiP.RequestId)
					<-PiP.Account.AwaitRequestResult(PiP.RequestId)
					log.Printf("Pingpong %s un-frozen due to result; re-evaluating.\n",
						PiP.Identifier())
					continue
				case "PLACED":
					if Requested.Uuid != "" && PiP.LastUuid != Requested.Uuid {
						PiP.RequestId = 0
						PiP.LastUuid = Requested.Uuid
						UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
						log.Printf("Pingpong %s: Request %d was placed but lastuuid was not updated. Addressing.\n",
							PiP.Identifier(), Requested.Id)
						continue
					}
				default:
					log.Fatalf("Pingpong %s made request %d; status is %s (unhandled)\n",
						PiP.Identifier(), PiP.RequestId)
				}
			} else {
				Requested, err = PiP.Account.GetRequest(PiP.RequestId)
				if err != nil {
					log.Fatalf("Pingpong %s: failure %s\n", err)
				}
				if Requested != nil {
					switch Requested.Status {
					case "PLACED":
						PiP.RequestId = 0
						PiP.LastUuid = Requested.Uuid
						UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
						log.Printf("Pingpong %s: Request %d was placed but lastuuid was not updated. Addressing.\n",
							PiP.Identifier(), Requested.Id)
						continue
					case "REJECTED":
						PiP.RequestId = 0
						PiP.LastUuid = Requested.Uuid
						UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
						log.Errorf("Pingpong %s: Request %d placed, and then rejected. Suspending to avoid thrashing.\n",
							PiP.Identifier(), Requested.Id)
						ChatBuffer(":table_tennis_paddle_and_ball::fire: Suspending pingpong %s (%s %s; %.8f @ %.8f) order rejected by %s!\n",
							PiP.Identifier(), PiP.Mode, PiP.Market.Name(), PiP.BuyQty, PiP.BuyLimit, PiP.Account.Exchange.Name)
						PiP.Suspend()
						continue
					default:
						log.Fatalf("Pingpong %s: Have request id %d, and response is in undefined state: %+v\n",
							PiP.Identifier(), PiP.RequestId, Requested)
						PiP.Suspend()
						continue
					}
				} else {
					log.Fatalf("Pingpong %s: Have request id %d, but no corresponding record.\n",
						PiP.Identifier(), PiP.RequestId)
				}
				ChatBuffer(":table_tennis_paddle_and_ball::fire: Pingpong %s (%s %s; %.8f @ %.8f) undefined status on requested order!\n",
					PiP.Identifier(), PiP.Mode, PiP.Market.Name(), PiP.BuyQty, PiP.BuyLimit)
				PiP.Suspend()
				continue
			}
		}
		log.Debugf("Pingpong %d (%s) checking to see if '%s' is still open...\n",
			PiP.Id, Market, LastUuid)
		Open := A.Cache.Open[LastUuid]
		var Closed *okane.Order
		if Open != nil {
			log.Printf("Pingpong %s: last order '%s' is still open; added watch for not-open state.\n",
				PiP.Identifier(), PiP.LastUuid)
			<-PiP.Account.AwaitUuidNotOpen(PiP.LastUuid)
			log.Printf("Pingpong %s un-frozen due to order vanishing; re-evaluating.\n",
				PiP.Identifier())
			continue
		}
		Closed, err = A.GetClosedOrder(LastUuid)
		if Closed == nil {
			log.Printf("Pingpong %s: Order '%s' vanished (cancelled?); time to reinstate.\n",
				PiP.Identifier(), PiP.LastUuid)
		} else {
			log.Printf("Pingpong %s: Order '%s'(%s) closed; time to flip.\n",
				PiP.Identifier(), PiP.LastUuid, Closed.Type)
			BuyAmt := PiP.BuyQty * PiP.BuyLimit
			SellAmt := PiP.SellQty * PiP.SellLimit
			Profit := SellAmt - BuyAmt
			ProfitPercent := (Profit / BuyAmt) * 100
			var NewMode string = PiP.Mode
			switch Closed.Type {
			case "BUY", "LIMIT_BUY":
				NewMode = "SELL"
				ChatBuffer(":table_tennis_paddle_and_ball: %s ping-pong %d (buy %.08f @ %.08f, sell %.08f @ %.08f; %.02f%% profit) has completed its buy phase. %.1f%% of the next sell's profit will be reinvested.",
					PiP.Market.Name(), PiP.Id, PiP.BuyQty, PiP.BuyLimit, PiP.SellQty, PiP.SellLimit,
					ProfitPercent, PiP.ReinvestRatio*100)
			case "SELL", "LIMIT_SELL":
				NewMode = "BUY"
				var ReinvestAmt float64
				var RestockQty float64
				var ReinvestQty float64
				var FinalSellQty float64
				//var FinalBuyAmt float64
				if PiP.ReinvestRatio != 0 {
					ReinvestAmt = Profit * PiP.ReinvestRatio
					ReinvestQty = ReinvestAmt / PiP.BuyLimit
				}
				if PiP.RestockRatio != 0 {
					RestockQty = ReinvestQty * PiP.RestockRatio
				}
				FinalBuyQty := PiP.BuyQty + ReinvestQty
				if PiP.RestockRatio != 0 {
					FinalSellQty = PiP.BuyQty + RestockQty
				} else {
					FinalSellQty = PiP.SellQty
				}
				//FinalBuyAmt = FinalBuyQty * PiP.BuyLimit
				PiP.BuyQty = FinalBuyQty
				PiP.SellQty = FinalSellQty
				ChatBuffer(":table_tennis_paddle_and_ball: %s ping-pong %d (buy %.8f @ %.8f, sell %.8f @ %.8f; %.02f%% profit) has sold; now reinvesting %.1f%% of %.8f profit into %.8f more coins (%.8f for stockpile).\n",
					PiP.Market.Name(), PiP.Id, PiP.BuyQty, PiP.BuyLimit, PiP.SellQty, PiP.SellLimit, ProfitPercent, PiP.ReinvestRatio*100, Profit, FinalBuyQty-PiP.BuyQty, FinalBuyQty-FinalSellQty)
			}
			PiP.Mode = NewMode
			_, err = RecordFlipTargets.Exec(PiP.Id, PiP.Mode, PiP.BuyQty, PiP.SellQty)
			log.ErrorIff(err, "Pingpong %s failed to update target flip")
			log.Printf("Recorded flip to mode '%s'\n", NewMode)
		}
		Req, err := PiP.AssertOrder()
		log.FatalIff(err, "failed to assert order for %s", PiP.Identifier())
		if err != nil {
			ErrClass := PiP.Account.User.Db.ErrorType(err)
			switch ErrClass {
			case "duplicate_key":
				log.Printf("Pingpong %s already requested order; suspending self.\n",
					PiP.Identifier())
				ChatBuffer(":table_tennis_paddle_and_ball::fire: Pingpong %s (%s; %.8f @ %.8f) key jammed up in database! Suspending strategy until it's resolved.\n",
					PiP.Identifier(), PiP.Market.Name(), PiP.BuyQty, PiP.BuyLimit)
				PiP.Suspend()
			default:
				log.Fatalf("Pingpong %s: Unknown db issue '%s'\n",
					PiP.Identifier(), err)
			}
		} else {
			log.Printf("Result of order assertion: %s; uuid assigned '%s'\n",
				Req.Status, Req.Uuid)
			switch Req.Status {
			case "REJECTED":
				log.Printf("Pingpong %s order %d was rejected; disabling.\n",
					PiP.Identifier(), Req.Id)
				ChatBuffer(":table_tennis_paddle_and_ball::fire: Pingpong %s (%s; %.8f @ %.8f) order rejected by exchange! Suspending strategy until it's resolved.\n",
					PiP.Identifier(), PiP.Market.Name(), PiP.BuyQty, PiP.BuyLimit)
				PiP.Suspend()
				UpdateUuids.Exec(PiP.Id, PiP.LastUuid, PiP.RequestId)
			case "PLACED":
			default:
				log.Fatalf("Unknown result set %s\n", Req.Status)
			}
			log.Printf("Pingpong %s: order '%s' has closed!\n",
				PiP.Identifier(), PiP.LastUuid)
			continue
		}
		log.Printf("Pingpong %s: order '%s' successfully placed.\n",
			PiP.Identifier(), Req.Uuid)
		time.Sleep(time.Minute)
	}
}

/*
   Requestid should be blank unless there is an outstanding request.
   If requestid is populated and there's a UUID at that entry, we move it to lastuuid (pingpong's job)
   If there is an open order with the UUID
*/

func (PiP *strat_pingpong) Suspend() {
	PiP.Suspended = true
	log.Printf("Pingpong %s - %d - suspending.\n", PiP.Identifier(), PiP.Id)
	SetSuspended := PiP.Account.X["sth_pingpong_setsuspended"].(*shared.Stmt)
	_, err := SetSuspended.Exec(PiP.Id, true)
	if err != nil {
		log.Fatalf("Pingpong %s failed to suspend because %s.\n", PiP.Identifier(), err)
	}
}

func main() {
	LoadConfigValues("/data/baytor/okane.ini")
	InitAndConnect()
	//var LastPingpong *strat_pingpong
	go OrderRequestThread()
	go ChatQueueThread()
	for _, User := range Global.Users {
		Accounts := User.LoadAllAccounts()
		for _, Account := range Accounts {
			go PingpongConfigThread(Account)
		}
	}
	//	TestInteractive(LastPingpong)
	for {
		time.Sleep(time.Hour)
	}
}
