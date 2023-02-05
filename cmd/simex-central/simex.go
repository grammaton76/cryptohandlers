package main

import (
	"flag"
	"fmt"
	_ "github.com/grammaton76/chattools/pkg/chat_output/sc_dbtable"
	"github.com/grammaton76/cryptohandlers/pkg/okane"
	"github.com/grammaton76/g76golib/pkg/sentry"
	"github.com/grammaton76/g76golib/pkg/shared"
	"github.com/grammaton76/g76golib/pkg/sjson"
	"github.com/grammaton76/g76golib/pkg/slogger"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

/*

Need to ensure that open orders which vanish are deleted from o_order_open
* Decom cron-minute/okane-orderlog.pl once this is done
* Need to insert into okane_balances

CREATE TABLE marketlast (exchangeid int not null, marketid int not null, timeat timestamp not null, basecoinid int not null, volume float, last float, bid float, ask float, lastusd float);
*/

var log slogger.Logger
var Sentries *sentry.SentryTeam

var Config shared.Configuration

var Cli struct {
	Inifile  string
	ReadOnly bool
	Debug    bool
}

var Chaterr *shared.ChatTarget

var Global struct {
	db                    *shared.DbHandle
	ChatHandle            *shared.ChatHandle
	ChatSection           string
	LocalStorage          string
	MarketDataFile        string
	MarketRatesFile       string
	MarketIntervalsFile   string
	MarketRatesUsFile     string
	MarketIntervalsUsFile string
	PidFile               string
	MarketDataCycle       int
	MarketRateCycle       int
	PullInterval          *shared.Stmt
	PullSummaryInterval   *shared.Stmt
	CheckSummaries        *shared.Stmt
	ReadOnly              bool
	UserNames             []string
	Accounts              []*SimexAccount
	EventChannel          *okane.EventChannel
	DiagCheckUser         string
	NoRateData            bool
	Timing                struct {
		Timeout time.Duration
		Poll    time.Duration
	}
	MirrorExchange string
	MirrorStart    time.Time
	MirrorStop     time.Time
	MirrorStep     time.Duration
	//PullIntervalUsd       *shared.Stmt
}

var Exchange okane.Exchange = okane.ExchangeSimulator

type SimexAccount struct {
	*okane.Account
	Handle  *SimexApiActor
	Notices *shared.ChatTarget
	Section string
}

func LoadConfigValues(Ini string) {
	_ = log.Init()
	slogger.SetLogger(&log)
	shared.SetLogger(&log)
	okane.SetLogger(&log)
	SentryLog := slogger.Logger{}
	err := SentryLog.Init()
	log.FatalIff(err, "setting up sentry heartbeat process")
	sentry.SetLogger(&SentryLog)
	SentryLog.SetThreshold(slogger.INFO)
	OtherIni := flag.String("inifile", "", "Specify an INI file for settings")
	ReadOnly := flag.Bool("readonly", false, "No writes to database or filesystem.")
	Debug := flag.Bool("debug", false, "Enable verbose debugging.")
	CheckUser := flag.String("checkuser", "", "Only pull data for specified user.")
	flag.Parse()
	Cli.Inifile = *OtherIni
	Cli.ReadOnly = *ReadOnly
	Global.DiagCheckUser = *CheckUser
	Cli.Debug = *Debug
	if Cli.Debug {
		log.SetThreshold(slogger.DEBUG)
	}
	Global.ReadOnly = Cli.ReadOnly
	if Cli.Inifile != "" {
		Ini = Cli.Inifile
	}
	if Global.DiagCheckUser != "" {
		Global.NoRateData = true
	}

	Config.LoadAnIni([]string{Ini}).OrDie("Couldn't load ini file")
	Global.PidFile = Config.GetStringOrDefault("simex.ratelogpid", "/data/statefiles/simex-central.pid", "Defaulting pid file")
	shared.ExitIfPidActive(Global.PidFile)
	Global.ChatSection = Config.GetStringOrDefault("simex.chathandle", "", "No chat channel defined; no notices will be sent!\n")
	Global.LocalStorage = Config.GetStringOrDie("simex.localstorage", "")
	Global.MarketDataFile = Config.GetStringOrDefault("simex.marketdata", Global.LocalStorage+"/marketdata-simex.json", "")
	Global.MarketRatesFile = Config.GetStringOrDefault("simex.marketrates", Global.LocalStorage+"/marketrates-simex.json", "")
	Global.MarketIntervalsFile = Config.GetStringOrDefault("simex.marketintervals", Global.LocalStorage+"/marketintervals-simex.json", "")
	Global.MarketRatesUsFile = Config.GetStringOrDefault("simex.usmarketrates", Global.LocalStorage+"/usmarketrates-simex.json", "")
	Global.MarketIntervalsUsFile = Config.GetStringOrDefault("simex.usmarketintervals", Global.LocalStorage+"/usmarketintervals-simex.json", "")
	Seconds := Config.GetIntOrDefault("simex.ordercheck_seconds", 60, "Defaulting to checking orders every 60 seconds.\n")
	Global.Timing.Poll = time.Second * time.Duration(Seconds)

	found, List := Config.GetString("okane.userlist")
	if found {
		Global.UserNames = strings.Split(List, ",")
	}
	if Global.ReadOnly == false {
		TestFile := sjson.NewJson()
		log.FatalIff(TestFile.TestWritable(Global.MarketRatesFile), "Cache write startup test: MarketRates")
		log.FatalIff(TestFile.TestWritable(Global.MarketRatesUsFile), "Cache write starup test: MarketRatesUs")
	}
	Global.MirrorExchange = Config.GetStringOrDie("simex.trackexchange", "Must specify which exchange to use as base.\n")
	Global.MirrorStart = Config.GetTimeOrDie("simex.mirrorstart", "Must specify what time to start the mirror at.\n")
	Global.MirrorStop = Config.GetTimeOrDie("simex.mirrorstop", "Must specify what time to stop the mirror at.\n")
	Global.MirrorStep = Config.GetDurationOrDie("simex.mirrortick", "Must specify how long a clock minute lasts.\n")
}

func InitAndConnect() {
	Global.Timing.Poll = time.Minute
	Global.Timing.Timeout = time.Minute * 2
	Sentries = sentry.NewSentryTeam()
	Sentries.TripwireFunc = func(st *sentry.Sentry) {
		Global.ChatHandle.SendDefaultf("Simex-central rebooting due to failure on check %d of sentry %s (%s)\n",
			st.Counter(), st.Identifier(), st.Notes())
	}
	Sentries.DefaultTtl = Global.Timing.Timeout
	Global.db = Config.ConnectDbKey("okane.centraldb").OrDie()
	log.FatalIff(okane.DbInit(Global.db), "dbinit failed.\n")
	Global.EventChannel = okane.NewEventChannel()
	if Global.ChatSection != "" {
		Global.ChatHandle = Config.NewChatHandle(Global.ChatSection).OrDie("failed to define chat handle")
		Chaterr = Global.ChatHandle.OutputChannel
	}
	Global.PullInterval = Global.db.PrepareOrDie("select marketid,max(last),min(last),max(lastusd),min(lastusd) from marketlast where exchangeid=1 and timeat>$1 group by marketid;")
	Global.PullSummaryInterval = Global.db.PrepareOrDie("select marketid,max(high),min(low),max(highusd),min(lowusd) from marketlast_hour where exchangeid=1 and timeat>$1 group by marketid;")
	Global.CheckSummaries = Global.db.PrepareOrDie("select timeat,exchangeid from marketlast_hour where exchangeid=1 and timeat>$1;")
	// Top level section lists out the sections where the keys are defined.
	for _, Section := range Global.UserNames {
		Username := Config.GetStringOrDie(Section+".username", "Username is required")
		log.Printf("Init'ing user '%s'\n", Section)
		found, sList := Config.GetString(Section + ".simex")
		if !found {
			log.Printf("No simex handles for user '%s'\n", Section)
			continue
		}
		UserDb := Config.ConnectDbKey(Section + ".database").OrDie()
		if UserDb.IsDead() {
			log.Printf("Cannot connect to db for '%s', skipping this user.\n", Section)
			continue
		}
		Lists := strings.Split(sList, ",")
		User := okane.NewUser(UserDb)
		User.Username = Username
		User.Notices = Config.ChatTargetFromKey(Section + ".notifications")
		if User.Notices == nil {
			log.Printf("User %s has no notifications channel defined; no chat notices will be sent.\n",
				User.Identifier())
		}
		User.Notices = Config.ChatTargetFromKey(Section + ".notifications")
		if User.Notices == nil {
			log.Printf("User %s has no notifications channel defined; no chat notices will be sent.\n",
				User.Identifier())
		}
		// Underneath the user are accounts, not users.
		for _, UserSection := range Lists {
			log.Printf("Scanning accounts for section '%s'; now doing '%s'\n", Section, UserSection)
			Accounts := User.LoadAccountsForExchange(okane.Exchanges.ByName["simex"])
			for _, Account := range Accounts {
				bAccount := ToSimexAccount(Account, Section)
				Global.Accounts = append(Global.Accounts, bAccount)
			}
		}
	}
	Global.EventChannel.Connect()
}

func FetchMarketTickers(api *SimexApiActor) {
	for true {
		Summaries, err := api.GetTicker("")
		log.FatalIff(err, "Failed to download market summaries.\n")
		MarketRates := make(okane.MarketRatesType)
		UpdateTime := time.Now()
		for _, k := range Summaries.List() {
			v := Summaries.Select(k)
			MarketName, nerr := v.Name()
			if nerr != nil {
				log.Printf("Failure examining ticker summaries: %s\n", nerr)
			}
			Market := v.MarketDef
			Quote := okane.MarketQuote{
				MarketDef:  v.MarketDef,
				Last:       v.Last,
				Bid:        v.Bid,
				Ask:        v.Ask,
				LastUpdate: UpdateTime,
			}
			MarketRates[MarketName] = &Quote
			Market.LastRate = &Quote
		}
		MarketRates.SetFiat(nil)
		MarketRatesUs := make(okane.MarketRatesType)
		for _, v := range MarketRates {
			MarketRates[v.MarketDef.Name()] = v
			if v.MarketDef.UsProhibited {
				continue
			}
			MarketRatesUs[v.MarketDef.Name()] = v
		}
		Global.MarketRateCycle++

		File := sjson.NewJson()
		FileUs := sjson.NewJson()
		for _, rate := range MarketRates {
			market := rate.MarketDef
			Name := market.Name()
			Rec := sjson.NewJson()
			Def := sjson.NewJson()
			if market.LastRate != nil {
				Rec["Last"] = market.LastRate.Last.StringFixed(8)
				Rec["Bid"] = market.LastRate.Bid.StringFixed(8)
				Rec["Ask"] = market.LastRate.Ask.StringFixed(8)
				Rec["Volume"] = market.LastRate.Volume.Decimal.StringFixed(8)
				Rec["UsdRate"] = market.LastRate.UsdRate.StringFixed(8)
			}
			Def["Status"] = market.Status
			if market.Exchange != nil {
				Def["Exchange"] = market.Exchange.Name
			}
			Def["Precision"] = market.Precision.String()
			Def["MarketId"] = market.LookupItem.Id()
			Def["UsProhibited"] = market.UsProhibited
			Def["BaseCoin"] = market.BaseCoin.Id()
			Def["QuoteCoin"] = market.QuoteCoin.Id()
			Def["BaseCoinName"] = market.BaseCoin.Name()
			Def["QuoteCoinName"] = market.QuoteCoin.Name()
			Def["Created"] = market.Created.String()
			Def["Notice"] = market.Notice
			Rec["MarketDef"] = Def
			File[Name] = Rec
			if market.UsProhibited {
				continue
			}
			FileUs[Name] = Rec
		}
		log.ErrorIff(File.WriteToFile(Global.MarketRatesFile), "writing out json file '%s'", Global.MarketRatesFile)
		log.ErrorIff(FileUs.WriteToFile(Global.MarketRatesUsFile), "writing out json file '%s'", Global.MarketRatesUsFile)
		MarketRates.WriteMarketLast(Global.db, okane.Exchanges.ById[okane.EXCHANGE_SIMEX])
		log.Printf("Writing market rates to '%s' due to changes\n", Global.MarketRatesFile)
		time.Sleep(time.Minute)
	}
}

func FetchMarketIntervals() {
	const Hour int = 3600
	var Intervals []int = []int{Hour, 3 * Hour, 6 * Hour, 12 * Hour, 24 * Hour, 48 * Hour, 72 * Hour, 24 * 7 * Hour}
	LastRuns := make([]time.Time, len(Intervals))
	var Cycles int
	IntervalData := make(map[int]map[string]*okane.MarketRange)
	IntervalDataUS := make(map[int]map[string]*okane.MarketRange)
	ShortestInterval := Intervals[0]
	log.Printf("Don't forget to put the intervals into a database record.\n")
	Sentry := Sentries.NewSentry("FetchMarketIntervals()").SetTTL(time.Duration(5) * time.Minute)
	for true {
		Cycles++
		log.Printf("Attempting to sort out interval data...\n")
		Now := time.Now()
		var RangeUpdates int
		for k, Interval := range Intervals {
			IntervalName := (time.Duration(Interval) * time.Second).String()
			Sentry.Checkin("cycle %d, interval %d", Cycles, Interval)
			var marketid int
			var max, min, maxusd, minusd float64
			if Cycles > 1 {
				Expires := LastRuns[k].Add(time.Duration(Interval/60) * time.Second)
				if Expires.After(Now) {
					log.Debugf("Skipping re-scan on %d until '%s'", Interval, Expires.String())
					continue
				}
			}
			Start := shared.FormatMysqlTime(Now.Add(-time.Duration(Interval) * time.Second))
			log.Printf("Starting interval query for '%s' at '%s'.\n", IntervalName, Start)
			QueryStart := time.Now()
			res, err := Global.PullInterval.Query(Start)
			log.FatalIff(err, "Error pulling interval rates for '%s' started at '%s'\n", IntervalName, Start)
			log.Printf("Successfully concluded pull for interval '%s' after '%s'\n",
				IntervalName, time.Now().Sub(QueryStart))
			Results := make(map[string]*okane.MarketRange)
			ResultsUS := make(map[string]*okane.MarketRange)
			var Count int
			var DeadMarkets []string
			for res.Next() {
				err = res.Scan(&marketid, &max, &min, &maxusd, &minusd)
				log.FatalIff(err, "Caw, error")
				MarketItem := okane.MarketLookup.ByIdOrDie(marketid)
				found, MarketDef := Exchange.Markets.ByName(MarketItem.Name())
				if !found {
					log.Debugf("Skipping interval data for removed market '%s'\n", MarketItem.Name())
					continue
				}
				Rec := okane.MarketRange{
					MarketDef:  MarketDef,
					MinRate:    decimal.NewFromFloat(min),
					MinRateUsd: decimal.NewFromFloat(minusd),
					MaxRate:    decimal.NewFromFloat(max),
					MaxRateUsd: decimal.NewFromFloat(maxusd),
				}
				Name := Rec.MarketDef.Name()
				if Interval == ShortestInterval && Cycles > 1 {
					for _, checkinterval := range Intervals[1:] {
						Target := IntervalData[checkinterval][Name]
						if Target == nil {
							continue
						}
						MinRate := decimal.Min(Rec.MinRate, IntervalData[checkinterval][Name].MinRate)
						if !Target.MinRate.Equals(MinRate) {
							log.Debugf("On market '%s' interval '%d', early update MinRate from '%s' to '%s'\n",
								Name, Interval, Target.MinRate, MinRate)
							Target.MinRate = MinRate
							RangeUpdates++
						}
						MinRateUsd := decimal.Min(Rec.MinRateUsd, IntervalData[checkinterval][Name].MinRateUsd)
						if !Target.MinRateUsd.Equals(MinRateUsd) {
							log.Debugf("On market '%s' interval '%d', early update MinRateUsd from '%s' to '%s'\n",
								Name, Interval, Target.MinRateUsd, MinRateUsd)
							Target.MinRateUsd = MinRateUsd
							RangeUpdates++
						}
						MaxRate := decimal.Max(Rec.MaxRate, IntervalData[checkinterval][Name].MaxRate)
						if !Target.MaxRate.Equals(MaxRate) {
							log.Debugf("On market '%s' interval '%d', early update MaxRate from '%s' to '%s'\n",
								Name, Interval, Target.MaxRate, MaxRate)
							Target.MaxRate = MaxRate
							RangeUpdates++
						}
						MaxRateUsd := decimal.Max(Rec.MaxRateUsd, IntervalData[checkinterval][Name].MaxRateUsd)
						if !Target.MaxRateUsd.Equals(MaxRateUsd) {
							log.Debugf("On market '%s' interval '%d', early update MaxRateUsd from '%s' to '%s'\n",
								Name, Interval, Target.MaxRateUsd, MaxRateUsd)
							Target.MaxRateUsd = MaxRateUsd
							RangeUpdates++
						}
					}
				}
				if Rec.MarketDef.UsProhibited {
					Results[Rec.MarketDef.Name()] = &Rec
				} else {
					Results[Rec.MarketDef.Name()] = &Rec
					ResultsUS[Rec.MarketDef.Name()] = &Rec
				}
				Count++
			}
			res.Close()
			if len(DeadMarkets) > 0 {
				log.Printf("Dead markets in interval data: %s\n", DeadMarkets)
			}
			IntervalData[Interval] = Results
			IntervalDataUS[Interval] = ResultsUS
			//log.Printf(" %d records retrieved.\n", Count)
			LastRuns[k] = Now
		}
		//os.Exit(3)
		Bob := sjson.NewJsonFromObject(&IntervalData)
		log.ErrorIff(Bob.WriteToFile(Global.MarketIntervalsFile), "writing out global market intervals to '%s'", Global.MarketIntervalsFile)
		Bob = sjson.NewJsonFromObject(&IntervalDataUS)
		log.ErrorIff(Bob.WriteToFile(Global.MarketIntervalsUsFile), "writing out US market intervals to '%s'", Global.MarketIntervalsUsFile)
		time.Sleep(time.Minute)
	}
}

func ToSimexAccount(acct *okane.Account, Section string) *SimexAccount {
	//AuthSection := Config.GetStringOrDie(Section+".simex", "Must have a list of simex accounts (even if empty)")
	//TODO: Support multiple simex's based upon other exchanges
	Bob := &SimexAccount{
		Account: acct,
		Section: Section,
	}
	Bob.Exchange = okane.Exchanges.ById[okane.EXCHANGE_SIMEX]
	return Bob
}

func (sa *SimexAccount) checkOrderLoop() {
	const STATE_OPEN_DBONLY int = 1    // This is NOT on the open order list but db has it. Notify vanished.
	const STATE_OPEN_NODB int = 2      // This IS open but the db hasn't stored it yet. Notify new.
	const STATE_OPEN_CONFIRMED int = 3 // It was in DB and confirmed still open.
	const STATE_DBOPEN_CLOSED int = 4  // This WAS open in db and we just found it in closed. Notify closed.
	OrderState := make(map[string]int)
	DbOpenOrders, err := sa.Account.GetOpenOrders()
	log.ErrorIff(err, "failed to get open orders from db for %s\n", sa.Identifier())
	for k := range DbOpenOrders.ByUuid {
		OrderState[k] = STATE_OPEN_DBONLY
	}
	orders, err := sa.Handle.GetClosedOrders()
	if err != nil {
		log.Errorf("Failed to fetch closed orders for %s from simex; reason: '%s'\n", sa.Identifier(), err)
		sa.User.Notices.SendfIfDef("Failed to fetch closed orders for %s from simex; reason: '%s'\n", sa.Identifier(), err)
	} else {
		log.Printf("Scanned %d closed orders for '%s'\n", len(orders.Orders), sa.Identifier())
		for _, o := range orders.Orders {
			//log.Printf("It seems that order id '%s' is NOT present in closed orders.\n", o.ID)
			if DbOpenOrders.ByUuid[o.Uuid] != nil {
				OrderState[o.Uuid] = STATE_DBOPEN_CLOSED
				if o != nil {
					err = o.RecordClosedOrder()
					log.ErrorIff(err, "Error closing order %s", o.Identifier())
					sa.User.Notices.SendfIfDef("%s", o.FormatOrderClosedChat())
				}
			}
			//		log.Printf("Downloaded: '%+v'\n", Order)
		}
	}
	orders, err = sa.Handle.GetOpenOrders()
	if err != nil {
		log.Errorf("Failed to fetch open orders for %s from simex; reason: '%s'\n", sa.Identifier(), err)
		sa.User.Notices.SendfIfDef("Failed to fetch open orders for %s from simex; reason: '%s'\n", sa.Identifier(), err)
		return
	} else {
		if len(orders.Orders) == 0 {
			log.Printf("Empty open order list downloaded, but no error was presented either!\n")
			return
		}
		//OpenUUIDs, Fills := okane.LoadOrdersToMaps(OrderQuery)
		for _, o := range orders.Orders {
			if OrderState[o.Uuid] == STATE_OPEN_DBONLY {
				OrderState[o.Uuid] = STATE_OPEN_CONFIRMED
			} else {
				OrderState[o.Uuid] = STATE_OPEN_NODB
			}
			//log.Printf("It seems that order id '%s' is NOT present in open orders.\n", o.ID)
			//log.Printf("UUID '%s' is record id %d\n", Order.Uuid, UUIDs[Order.Uuid])
			if DbOpenOrders.ByUuid[o.Uuid] != nil {
				dbOrder := DbOpenOrders.ByUuid[o.Uuid]
				log.Debugf("Confirmed id '%s' is still present in open orders.\n", o.Uuid)
				if !dbOrder.Filled.Equal(o.Filled) {
					log.Debugf("Partial fill since the last update of o_order_open!\n")
					sa.OrderUpdatePartialFill(o)
					dbOrder.Filled = o.Filled
					log.ErrorIff(sa.Publish(okane.EV_ORDER_UPDATE, o), "error recording open order for simex")
				}
			} else {
				err = o.RecordOpenOrder()
				log.ErrorIff(err, "failed to record open order: %s\n", o.Identifier())
				sa.User.Notices.SendfIfDef("%s", o.FormatOrderOpenChat())
			}
		}
		for uuid := range OrderState {
			if OrderState[uuid] == STATE_OPEN_DBONLY {
				log.Printf("It is time to delete order '%s'; it is not open but is in o_order_open still.\n", uuid)
				err = sa.Account.User.RemoveOpenOrder(uuid)
				log.ErrorIff(err, "failed to remove open order %s\n", uuid)
				sa.Account.User.Notices.SendfIfDef("%s", DbOpenOrders.ByUuid[uuid].FormatOrderVanished())
			}
		}
		log.Printf("Scanned %d open orders for '%s'\n", len(orders.Orders), sa.Identifier())
	}
}

func (ba *SimexAccount) PlaceOrder(o *okane.ActionRequest) error {
	//uuid, err := ba.Handle.BuyLimit(o.Market.Name(), decimal.NewFromFloat(o.Qty), decimal.NewFromFloat(o.Bidlimit))
	switch o.Command {
	case "CANCEL":
		log.Printf("Attempting to cancel uuid '%s'\n", o.Uuid)
		_, err := ba.Handle.CancelOrder(o.Uuid)
		if err != nil {
			log.Errorf("Cancellation error executing order %s: %s\n",
				o.Identifier(), err)
			_, errdb := ba.Account.SetRequest(o, "PLACING", "REJECTED")
			log.ErrorIff(errdb, "Order cancellation request %s: failed to record rejection because", o.Identifier())
			return Chaterr.LogErrorf("Cancellation error executing order %s: %s\n",
				o.Identifier(), err)
		}
		err = o.MarkAsHandled("PLACED")
		log.ErrorIff(err, "Order request %s: failed to record rejection because")
		Chaterr.Sendf("Cancellation order '%s' successful.\n", o.Identifier())
		return nil
	}
	log.Printf("Placing order request %d: '%s' on %s for %.08f at %.08f\n",
		o.Id, o.Command, o.Market.Name(), o.Qty, o.Bidlimit)
	res, err := ba.Handle.PlaceOrder(o)
	if err != nil {
		_, errdb := ba.Account.SetRequest(o, "PLACING", "REJECTED")
		log.ErrorIff(errdb, "Order request %s: failed to record rejection because", o.Identifier())
		return Chaterr.LogErrorf("Placement error on order %s: %s\n",
			o.Identifier(), err)
	}
	o.Uuid = res.Uuid
	log.Printf("Result: %+v\n", res)
	err = o.MarkAsHandled("PLACED")
	log.ErrorIff(err, "Order request %s: successful order placement",
		o.Identifier())
	return nil
}

func (sa *SimexAccount) ProcessRequestedOrders() {
	var Cycles int
	for true {
		time.Sleep(time.Duration(5) * time.Second)
		Cycles++
		log.Printf("Order request cycle %d\n", Cycles)
		requests, err := sa.GetOpenOrderRequests()
		if err != nil {
			log.Printf("Account %s: Failed to fetch pending order requests: %s.\n",
				sa.Identifier(), err)
			return
		}
		var taken bool
		for _, Order := range requests {
			taken, err = sa.Account.SetRequest(Order, "REQUESTED", "PLACING")
			if err != nil {
				log.Printf("Account %s error taking order request: %s\n",
					sa.Identifier(), err)
				continue
			}
			if !taken {
				log.Printf("Account %s: Wasn't able to take order_request %d; may have lost election.\n", sa.Identifier(), Order.Id)
				continue
			}
			err = sa.PlaceOrder(Order)
			if err != nil {
				log.Printf("Account %s error processing order request %d: %s\n",
					sa.Identifier(), Order.Id, err)
				taken, err = sa.Account.SetRequest(Order, "PLACING", "REJECTED")
				if err != nil {
					log.Printf("Account %s error marking order request %d rejected: %s\n",
						sa.Identifier(), Order.Id, err)
					continue
				}
			} else {
				log.Printf("Account %s received uuid '%s' back from orderrequest %d\n",
					sa.Identifier(), Order.Uuid, Order.Id)
				err = sa.Account.RecordStratOrder(Order)
				log.ErrorIff(err, "Account %s to record strat order", sa.Identifier())
			}
		}
	}
}

func SimexUserHandler(Simex *SimexAccount) {
	Id := fmt.Sprintf("SimexUserHandler(%s)", Simex.Identifier())
	Sentry := Sentries.NewSentry(Id)
	log.Printf("Init for user '%s'\n", Simex.Identifier())
	Simex.Account.User.Events = Global.EventChannel
	go Simex.ProcessRequestedOrders()
	for true {
		log.Debugf("Scanning orders for '%s'\n", Simex.Identifier())
		Sentry.Checkin("checkOrderLoop()")
		Simex.checkOrderLoop()
		Sentry.Checkin("ProcessRequestedOrders()")
		Sentry.Checkin("Time sleep waiting for next cycle")
		time.Sleep(Global.Timing.Poll)
	}
}

func FetchMarketDefs() {
}

func main() {
	LoadConfigValues("/data/config/simex-central.ini")
	InitAndConnect()
	Chaterr.SendfIfDef("Simex central process bootup.\n")
	var NoApi *SimexApiActor
	//NoApi.SetDebug(false)
	FetchMarketDefs()
	for Global.MarketDataCycle == 0 {
		time.Sleep(time.Second)
	}
	if !Global.NoRateData {
		go FetchMarketIntervals()
		go FetchMarketTickers(NoApi)
	}
	for Global.MarketRateCycle == 0 {
		time.Sleep(time.Second)
	}
	log.Printf("Market definitions loaded.\n")
	for _, User := range Global.Accounts {
		if Global.DiagCheckUser != "" {
			if User.Section == Global.DiagCheckUser {
				SimexUserHandler(User)
			} else {
				log.Debugf("Skipping spawn of account thread on user '%s'\n", User.Identifier())
			}
		} else {
			go SimexUserHandler(User)
		}
	}
	for true {
		time.Sleep(time.Hour)
	}
}
