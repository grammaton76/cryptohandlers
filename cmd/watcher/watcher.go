package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/grammaton76/g76golib/chatoutput/sc_dbtable"
	"github.com/grammaton76/g76golib/okane"
	"github.com/grammaton76/g76golib/sentry"
	"github.com/grammaton76/g76golib/shared"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/grammaton76/g76golib/slogger"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

// TODO: Copy the stratorder checking from log-to-slack.pl - track completion of orders from strategies.
// TODO: Chain order code needs written.

/*

create table stratprofit_track(id serial not null primary key, stratowner int, lastclose timestamp, reboot bool default false);

create index stratcloses on stratorders (stratowner,closed);

Categorize each buy and sell by stairstep and treat each stair as if it were a pingpong. Ensure there's some kind of bleeding or
splash damage to avoid pinpoint concentrations but do the pingpong thing with last 100 orders. Max out the strategic effect at 8 steps?
Also watch for skipped steps, a sure sign of missed profit. Need to establish a mechanism to harness sells above and buys below too.
Maybe be tunable on markets to always be watching for a +25% spike for instance.

*/

var log slogger.Logger

var Config shared.Configuration

var Sentries *sentry.SentryTeam

var Cli struct {
	Inifile  string
	ReadOnly bool
	Debug    bool
}

var rQ struct {
	PullInterval   *shared.Stmt
	CheckSummaries *shared.Stmt
	GetExchanges   *shared.Stmt
}

var wQ struct {
	InsertSummary *shared.Stmt
}

var Global struct {
	PidFile       string
	db            *shared.DbHandle
	MetaCycle     int
	ChatHandle    *shared.ChatHandle
	ChatSection   string
	ChatDiags     *shared.ChatTarget
	EventChannel  *okane.EventChannel
	Users         map[string]*okane.User
	DiagCheckUser string
	ReadOnly      bool
	Timing        struct {
		SentryTimeout    time.Duration
		DataPullInterval time.Duration
		UserPullInterval time.Duration
	}
	SummaryRange time.Duration
	Squawks      Squawks
}

type Squawks struct {
	msgs map[string]string
}

func (s *Squawks) Init() {
	s.msgs = make(map[string]string)
}

func (s *Squawks) Set(key string, status string) {
	s.msgs[key] = status
}

func (s *Squawks) Setf(key string, format string, options ...interface{}) {
	s.msgs[key] = fmt.Sprintf(format, options...)
}

func (s *Squawks) Clear(key string) {
	delete(s.msgs, key)
}

type HourSummary struct {
	ExchangeId int
	MarketId   int
	TimeAt     time.Time
	CoinId     int
	BasecoinId int
	High       float64
	Low        float64
	HighUsd    float64
	LowUsd     float64
}

func (s *Squawks) Summary() string {
	var M []string
	for _, v := range s.msgs {
		M = append(M, v)
	}
	return strings.Join(M, "\n")
}

func InitAndConnect() {
	Global.Timing.SentryTimeout = time.Minute * 2
	Global.Squawks.Init()
	Sentries = sentry.NewSentryTeam()
	Sentries.TripwireFunc = func(st *sentry.Sentry) {
		Global.ChatHandle.SendDefaultf("Kraken-central rebooting due to failure on check %d of sentry %s (%s)\n",
			st.Counter(), st.Identifier(), st.Notes())
	}
	Sentries.DefaultTtl = Global.Timing.SentryTimeout
	Global.Users = make(map[string]*okane.User)
	Global.db = Config.ConnectDbKey("okane.centraldb").OrDie()
	log.FatalIff(okane.DbInit(Global.db), "dbinit failed.\n")

	rQ.CheckSummaries = Global.db.PrepareOrDie(
		"SELECT timeat,exchangeid FROM marketlast_hour where timeat>$1;")
	rQ.PullInterval = Global.db.PrepareOrDie(
		"SELECT marketid,max(last),min(last),max(lastusd),min(lastusd) FROM marketlast where timeat>=$2 and timeat<$3 and exchangeid=$1 group by marketid;")
	rQ.GetExchanges = Global.db.PrepareOrDie(
		`SELECT distinct(exchangeid) FROM marketlast where timeat>now()-'8 hour'::interval;`)
	wQ.InsertSummary = Global.db.PrepareOrDie(
		`INSERT INTO marketlast_hour (exchangeid,marketid,timeat,coinid,basecoinid,high,low,highusd,lowusd) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`)

	Global.ChatHandle = Config.NewChatHandle(Global.ChatSection).OrDie("failed to define chat handle")
	Global.ChatDiags = Global.ChatHandle.OutputChannel
	Global.EventChannel = okane.NewEventChannel()
	Global.EventChannel.Connect()
	Global.EventChannel.FpReceiver = EventReceiver
	Global.SummaryRange = time.Duration(24) * time.Hour * 28
	var SubscribeChannels []string = []string{"market"}
	found, List := Config.GetString("okane.userlist")
	if !found {
		log.Printf("No okane.userlist defined! This system will only watch the general market and will not be very useful.\n")
	} else {
		UserNames := strings.Split(List, ",")
		for _, Section := range UserNames {
			Username := Config.GetStringOrDie(Section+".username", "No username present")
			SubscribeChannels = append(SubscribeChannels, "user_"+Username)
			log.Printf("Scanning accounts for user '%s' defined in section '%s'\n", Username, Section)
			UserDb := Config.ConnectDbKey(Section + ".database").OrDie()
			if UserDb.IsDead() {
				log.Printf("Cannot connect to db for '%s', skipping this user.\n", Section)
				continue
			}
			User := okane.NewUser(UserDb)
			User.Username = Username
			User.Events = Global.EventChannel
			User.Accounts = User.LoadAllAccounts()
			if Global.ChatHandle != nil {
				found, channel := Config.GetString(Section + ".chattarget")
				if found {
					User.Notices = Global.ChatHandle.ChatTarget(channel)
					if User.Notices == nil {
						log.Fatalf("Nil chat target for handle '%s' channel '%s'\n", Global.ChatHandle.Identifier(), channel)
					}
				} else {
					log.Printf("No '%s.chattarget' defined no trade chats etc.\n", Section)
				}
			}
			Global.Users[Username] = User
		}
	}
	go Global.EventChannel.StartListener(SubscribeChannels...)
}

func UserHandler(User *okane.User) {
	var Cycle int
	type StratHistory struct {
		Profits   map[*okane.Coin]map[float64]decimal.Decimal
		LastClose time.Time
	}
	Sentry := Sentries.NewSentry("UserHandler-%s", User.Username)
	StratMap, err := User.GetStrats()
	log.FatalIff(err, "Error on user %s; skipping this user", Sentry)
	Strats := StratMap.ToMap()
	User.Events.SubscribeChannels(User.Username)
	var PrevLastCloses map[int]time.Time
	for true {
		Cycle++
		Sentry.Activate()
		Sentry.Checkin("cycle %d", Cycle)
		log.Printf("User '%s' cycle %d\n", User.Username, Cycle)
		LastCloses := User.LastStratCloses()
		for StratId, LastClose := range LastCloses {
			Strat := Strats[StratId]
			if Strat == nil {
				log.Debugf("strategy '%d' referenced in db but not found in memory. Skipping.\n",
					StratId)
				continue
			}
			if LastClose.Equal(PrevLastCloses[StratId]) {
				log.Debugf("No new closes on strat '%s'; skipping.\n",
					Strat.Identifier())
				continue
			}
			log.Debugf("Need to analyze profits for strat %d (%s)\n",
				Strat.Id, Strat.Market.Name())
			History, err := Strat.GetOrderList()
			log.FatalIff(err, "Failed pulling order history for '%s'\n", Strat.Identifier())
			if len(History.Orders) > 0 {
				log.Debugf("Order barf: %s\n", History)
			}
			History.GetPL()
		}
		log.Printf("Finished user '%s' loop; sleeping.\n", User.Identifier())
		Sentry.Deactivate()
		time.Sleep(Global.Timing.UserPullInterval)
		PrevLastCloses = LastCloses
	}
}

func LoadConfigValues(Ini string) {
	log.Init()
	slogger.SetLogger(&log)
	shared.SetLogger(&log)
	okane.SetLogger(&log)
	SentryLog := slogger.Logger{}
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
	Config.LoadAnIni(Ini).OrDie()
	Global.PidFile = Config.GetStringOrDefault("okane.watcherpid", "/data/baytor/state/watcher.pid", "Defaulting pid file")
	shared.ExitIfPidActive(Global.PidFile)

	Global.ChatSection = Config.GetStringOrDie("watcher.chathandle", "No chat section defined. This is a big issue on a watcher")
	Seconds := Config.GetIntOrDefault("watcher.usercheck_seconds", 60, "Defaulting to checking user orders every 60 seconds.\n")
	Global.Timing.UserPullInterval = time.Second * time.Duration(Seconds)
	Seconds = Config.GetIntOrDefault("watcher.metacheck_seconds", 60, "Defaulting to checking data updates every 60 seconds.\n")
	Global.Timing.DataPullInterval = time.Second * time.Duration(Seconds)
}

func ChatAboutOrder(user *okane.User, Reason string, o *okane.Order) string {
	var TradeFmt, Trade, ActionEmoji, Verb string
	var IsBuy, IsSell bool
	var EndTime time.Time
	// ^ We need to consult strategy orders to see if this was pingpong, etc.
	switch o.Type {
	case "LIMIT_SELL":
		TradeFmt = "limit-sell %s-%s"
		IsSell = true
	case "LIMIT_BUY":
		TradeFmt = "limit-buy %s-%s"
		IsBuy = true
	case "MARKET_SELL":
		TradeFmt = "market-sell %s-%s"
		IsSell = true
	case "MARKET_BUY":
		TradeFmt = "market-buy %s-%s"
		IsBuy = true
	default:
		Trade = fmt.Sprintf("%s in %s-%s", o.Type, o.BaseCoin, o.QuoteCoin)
	}
	if TradeFmt != "" {
		Trade = fmt.Sprintf(TradeFmt, o.BaseCoin, o.QuoteCoin)
	}
	Amounts := fmt.Sprintf("%s @ %s",
		o.Quantity.StringFixed(8), o.Bidlimit.StringFixed(8))
	ActionEmoji = ":question:"
	BeforeAmounts := ""
	switch Reason {
	case "orderopen":
		ActionEmoji = ":calendar:"
		Verb = "Placed"
		Amounts += fmt.Sprintf(" (max %s)", (o.Quantity.Mul(o.Bidlimit)).StringFixed(8))
	case "orderclosed", "orderupdate":
		Amounts += fmt.Sprintf(" (%s / $%s)", o.TotalPrice.StringFixed(8), (o.Filled.Mul(o.UsdTotal)).StringFixed(8))
		if IsSell {
			ActionEmoji = ":heavy_dollar_sign:"
		}
		if IsBuy {
			ActionEmoji = ":truck:"
		}
		if Reason == "orderupdate" {
			Verb = "Partially filled"
			EndTime = time.Now()
			BeforeAmounts = fmt.Sprintf("%s/", o.Filled.StringFixed(8))
		} else {
			Verb = "Executed"
			EndTime = o.Closed
		}
	}
	Msg := fmt.Sprintf(":eye:%s %s %s %s%s on %s", ActionEmoji, Verb, Trade, BeforeAmounts, Amounts, o.Account.Exchange.Name)
	if !o.Closed.IsZero() {
		//Delta := o.Created.Sub(EndTime)
		Delta := EndTime.Sub(o.Created)
		Msg += fmt.Sprintf(" after %dd %d:%02d:%02d", int(Delta.Hours()/24), pmod(int(Delta.Hours()), 24), pmod(int(Delta.Minutes()), 60), pmod(int(Delta.Seconds()), 60))
	}
	Msg += "\n"
	log.Printf("Generated chat notification: %s\n", Msg)
	return Msg
}

func pmod(x, d int) int {
	x = x % d
	if x >= 0 {
		return x
	}
	if d < 0 {
		return x - d
	}
	return x + d
}

func EventReceiver(channel string, data []byte) error {
	//log.Printf("Channel '%s' says '%s'\n", channel, string(data))
	Rec := sjson.NewJsonFromString(string(data))
	if Global.Users[channel] != nil {
		User := Global.Users[channel]
		Reason := Rec.KeyString("Reason")
		Type := Rec.KeyString("ObjType")
		Type = strings.TrimLeft(Type, "*")
		var Order *okane.Order
		switch Type {
		case "okane.Order":
			Order = &okane.Order{}
			log.ErrorIff(json.Unmarshal(data, &Order), "failed to unmarshal order")
		}
		switch Reason {
		case "orderopen", "orderupdate", "orderclosed":
			log.Printf("Order has happened for '%s': %+v\n", channel, string(data))
			if User.Notices != nil {
				log.Printf("Generating chat notification for '%s'\n", User.Identifier())
				User.Notices.Sendf(ChatAboutOrder(User, Reason, Order))
			} else {
				log.Printf("No chat target defined for '%s'; skipping update\n", User.Identifier())
			}
		default:
			log.Printf("Received undefined reason '%s'\n", Reason)
		}
	}
	return nil
}

func MaintainSummaries() {
	log.Errorf("Intentional lockup here; remove comment once summary generation works.\n")
	Cycles := 0
	FirstSummary := time.Now().Add(-Global.SummaryRange)
	Sel, err := rQ.CheckSummaries.Query(FirstSummary)
	if err != nil {
		log.Errorf("Failed to pull summaries; disabling summary generation - %s\n", err)
		Global.Squawks.Set("summaries", "summary generation offline due to query failure")
	}
	Summaries := make(map[int]map[int64]SumStatus)
	exchangehits := make(map[int]int)
	exchangehits[1] = 1
	var timeat time.Time
	var exchangeid int
	for Sel.Next() {
		err = Sel.Scan(&timeat, &exchangeid)
		if err != nil {
			log.Errorf("Failed to scan summaries: %s\n", err)
			Global.Squawks.Set("summaries", "summary generation offline due to scan failure")
		}
		Slot := timeat.Unix()
		Slot -= Slot % 3600
		if Summaries[exchangeid] == nil {
			Summaries[exchangeid] = make(map[int64]SumStatus)
		}
		Summaries[exchangeid][Slot] = DONE
		exchangehits[exchangeid]++
	}
	FirstSlot := FirstSummary.Unix() / 3600
	log.Printf("It is time to render the summaries for each exchange all the way back to %s (slot %d)\n",
		FirstSummary.String(), FirstSlot)
	LastSlot := (time.Now().Unix() / 3600) - 2
	//LastTime := time.Unix(LastSlot*3600, 0)
	for exchangeid = range exchangehits {
		slot := FirstSlot
		log.Printf("Processing summaries for exchange %d - slot %d to %d\n",
			exchangeid, FirstSlot, LastSlot)
		for slot < LastSlot {
			SlotStart := time.Unix(slot*3600, 0)
			SlotEnd := time.Unix((slot+1)*3600, 0)
			if Summaries[exchangeid][slot] == UNKNOWN {
				var res *sql.Rows
				res, err = rQ.PullInterval.Query(exchangeid, SlotStart, SlotEnd)
				if err != nil {
					log.Errorf("Failed to pull interval on exchange %d from '%s' - '%s'\n",
						exchangeid, SlotStart.String(), SlotEnd.String())
					Global.Squawks.Set("summaries", "interval pull query failure")
				} else {
					log.Printf("Identified that exchange %d, slot %d ('%s' - '%s') needs rendered.\n",
						exchangeid, slot, SlotStart.String(), SlotEnd.String())
				}
				for res.Next() {
					var S HourSummary
					err = res.Scan(&S.ExchangeId, &S.MarketId, &S.TimeAt, &S.CoinId,
						&S.BasecoinId, &S.High, &S.Low, &S.HighUsd, &S.LowUsd)
					if err != nil {
						Global.Squawks.Set("summaries", "summary pull error on scan")
					}
					_, err = wQ.InsertSummary.Exec(S.ExchangeId, S.MarketId, S.TimeAt, S.CoinId,
						S.BasecoinId, S.High, S.Low, S.HighUsd, S.LowUsd)
					if err != nil {
						Global.Squawks.Set("summaries", "summary insert error")
					} else {
						Summaries[exchangeid][slot] = DONE
					}
				}
			}
			slot++
		}
	}
	for true {
		Cycles++
		time.Sleep(time.Hour)
	}
}

type SumStatus int

const UNKNOWN SumStatus = 0
const DONE SumStatus = 1
const PENDING SumStatus = 2

func main() {
	LoadConfigValues("/data/baytor/okane.ini")
	InitAndConnect()
	Global.ChatDiags.SendfIfDef("Okane watcher central process bootup.\n")
	MaintainSummaries()
	/*go MaintainMetadata()
	for Global.MetaCycle == 0 {
		time.Sleep(time.Second)
	}*/
	// foreach exchanges
	//   Check yesterday (midnight-to-midnight), publish high/low summary data by marketid and exchangeid
	//   Write backload process to check and do this going back one week or so.
	// Watcher should watch for rate changes and update short-term summary tables too, obviating the need for range queries.
	for _, User := range Global.Users {
		if Global.DiagCheckUser != "" {
			if User.Username == Global.DiagCheckUser {
				UserHandler(User)
			} else {
				log.Debugf("Skipping spawn of account thread on account '%s'\n", User.Username)
			}
		} else {
			go UserHandler(User)
		}
	}
	for true {
		time.Sleep(time.Hour)
	}
}
