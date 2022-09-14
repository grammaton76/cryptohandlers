package main

import (
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
	Accounts              []*BittrexAccount
	EventChannel          *okane.EventChannel
	DiagCheckUser         string
	NoRateData            bool
	Timing                struct {
		Timeout time.Duration
		Poll    time.Duration
	}
	//PullIntervalUsd       *shared.Stmt
}

var Exchange okane.Exchange = okane.ExchangeBittrex

type BittrexAccount struct {
	*okane.Account
	Handle  *bittrex.Bittrex
	Notices *shared.ChatTarget
	Section string
}

var CoinLookup shared.LookupTable

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

	Config.LoadAnIni(Ini).OrDie()
	Global.PidFile = Config.GetStringOrDefault("bittrex.ratelogpid", "/data/baytor/state/bittrex-central.pid", "Defaulting pid file")
	shared.ExitIfPidActive(Global.PidFile)
	Global.ChatSection = Config.GetStringOrDefault("bittrex.chathandle", "", "No chat channel defined; no notices will be sent!\n")
	Global.LocalStorage = Config.GetStringOrDie("bittrex.localstorage", "")
	Global.MarketDataFile = Config.GetStringOrDefault("bittrex.marketdata", Global.LocalStorage+"/marketdata-bittrex.json", "")
	Global.MarketRatesFile = Config.GetStringOrDefault("bittrex.marketrates", Global.LocalStorage+"/marketrates-bittrex.json", "")
	Global.MarketIntervalsFile = Config.GetStringOrDefault("bittrex.marketintervals", Global.LocalStorage+"/marketintervals-bittrex.json", "")
	Global.MarketRatesUsFile = Config.GetStringOrDefault("bittrex.usmarketrates", Global.LocalStorage+"/usmarketrates-bittrex.json", "")
	Global.MarketIntervalsUsFile = Config.GetStringOrDefault("bittrex.usmarketintervals", Global.LocalStorage+"/usmarketintervals-bittrex.json", "")
	Seconds := Config.GetIntOrDefault("bittrex.ordercheck_seconds", 60, "Defaulting to checking orders every 60 seconds.\n")
	Global.Timing.Poll = time.Second * time.Duration(Seconds)

	found, List := Config.GetString("okane.userlist")
	if found {
		Global.UserNames = strings.Split(List, ",")
	}
}

func InitAndConnect() {
	Global.Timing.Poll = time.Minute
	Global.Timing.Timeout = time.Minute * 2
	Sentries = sentry.NewSentryTeam()
	Sentries.TripwireFunc = func(st *sentry.Sentry) {
		Global.ChatHandle.SendDefaultf("Bittrex-central rebooting due to failure on check %d of sentry %s (%s)\n",
			st.Counter(), st.Identifier(), st.Notes())
	}
	Sentries.DefaultTtl = Global.Timing.Timeout
	Global.db = Config.ConnectDbKey("okane.centraldb").OrDie()
	log.FatalIff(okane.DbInit(Global.db), "dbinit failed.\n")
	CoinLookup = shared.NewLookup("coinlookup", Global.db)
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
		found, sList := Config.GetString(Section + ".bittrex")
		if !found {
			log.Printf("No bittrex handles for user '%s'\n", Section)
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
			Accounts := User.LoadAccountsForExchange(okane.Exchanges.ByName["bittrex"])
			for _, Account := range Accounts {
				bAccount := ToBittrexAccount(Account, Section)
				Global.Accounts = append(Global.Accounts, bAccount)
			}
		}
	}
	Global.EventChannel.Connect()
}

func FetchMarketDefs(api *bittrex.Bittrex) {
	NewMarkets := okane.NewMarketDef()
	OldMarkets := okane.NewMarketDef()
	Sentry := Sentries.NewSentry("FetchMarketDefs()").SetTTL(time.Duration(5) * time.Minute)
	var Cycles, LastSuccess int
	for true {
		Cycles++
		Sentry.Checkin("market fetch, cycle %d", Cycles)
		result, err := api.GetMarkets()
		if err != nil {
			if LastSuccess < (Cycles - 3) {
				log.FatalIff(err, "Fatal error on market grab.\n")
			}
			log.Errorf("Failed market fetch %d (last success: %d): %s\n", Cycles, LastSuccess, err)
			time.Sleep(time.Minute)
			continue
		}
		for _, v := range result {
			Name := v.Symbol
			Market := okane.MarketDef{
				BaseCoin:     okane.CoinLookup.ByNameOrAdd(v.BaseCurrencySymbol),
				QuoteCoin:    okane.CoinLookup.ByNameOrAdd(v.QuoteCurrencySymbol),
				Status:       v.Status,
				Created:      v.CreatedAt,
				UsProhibited: false,
				Notice:       v.Notice,
				Precision:    decimal.NewFromInt(1).Shift(-v.Precision),
			}
			if OldMarkets[Name] != nil {
				Market.LastRate = OldMarkets[Name].LastRate
			}
			Market.SetSymbol(Name)
			for _, v := range v.ProhibitedIn {
				if v == "US" {
					Market.UsProhibited = true
				}
			}
			NewMarkets[Name] = &Market
		}
		Exchange.Markets = NewMarkets
		Global.MarketDataCycle++
		LastSuccess = Cycles
		if NewMarkets.Equals(&OldMarkets) {
			log.Printf("Market data in '%s' has had no changes\n", Global.MarketDataFile)
		} else {
			err = NewMarkets.UpdateOmarketsTable(okane.EXCHANGE_BITTREX)
			log.FatalIff(err, "failed to update omarket table in database")

			File := sjson.NewJson()
			for _, market := range NewMarkets {
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
				File[Name] = Def
			}
			log.ErrorIff(File.WriteToFile(Global.MarketDataFile), "writing out json file '%s'", Global.MarketDataFile)
			log.Printf("Writing market data to '%s' due to changes\n", Global.MarketDataFile)
		}
		Sentry.Checkin("post-fetch sleep, waiting for cycle %d", Cycles+1)
		time.Sleep(time.Minute)
		OldMarkets = NewMarkets
	}
}

func FetchMarketTickers(api *bittrex.Bittrex) {
	for true {
		Summaries, err := api.GetTicker("")
		log.FatalIff(err, "Failed to download market summaries.\n")
		MarketRates := make(okane.MarketRatesType)
		UpdateTime := time.Now()
		for _, v := range Summaries {
			MarketName := okane.MarketLookup.ByNameOrAdd(v.Symbol).Name()
			Market := Exchange.Markets.ByNameOrDie(MarketName)
			Quote := okane.MarketQuote{
				MarketDef:  Market,
				Last:       v.LastTradeRate,
				Bid:        v.BidRate,
				Ask:        v.AskRate,
				LastUpdate: UpdateTime,
			}
			MarketRates[v.Symbol] = &Quote
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
		MarketRates.WriteMarketLast(Global.db, okane.Exchanges.ById[okane.EXCHANGE_BITTREX])
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

func ToBittrexAccount(acct *okane.Account, Section string) *BittrexAccount {
	AuthSection := Config.GetStringOrDie(Section+".bittrex", "Must have a list of bittrex accounts (even if empty)")
	Bob := &BittrexAccount{
		Account: acct,
		Section: Section,
	}
	Bob.Exchange = okane.Exchanges.ById[okane.EXCHANGE_BITTREX]
	Key := Config.GetStringOrDie(AuthSection+".key", "Key is required.\n")
	Secret := Config.GetStringOrDie(AuthSection+".secret", "Secret is required.\n")
	Bob.Handle = bittrex.New(Key, Secret)
	return Bob
}

func (ba *BittrexAccount) UpdateBalances() {
	Data, err := ba.Handle.GetBalances()
	log.FatalIff(err, "Failed to get balances on %s\n", ba.Identifier())
	Minimum, _ := decimal.NewFromString("0.00000001")
	Bal := ba.NewBalanceSnapshot()
	for _, v := range Data {
		Balance := v.Total
		Available := v.Available
		Hold := Balance.Sub(Available)
		if Balance.LessThanOrEqual(Minimum) {
			continue
		}
		CoinId := CoinLookup.LabelToId(v.CurrencySymbol, true).Id()
		Bal.Add(CoinId, Balance.Round(8), Available.Round(8), Hold.Round(8))
	}
	log.ErrorIff(Bal.UpdateDb(), "failed to write balance snapshot")
}

func (ba *BittrexAccount) BittrexOrderToOkane(o *bittrex.OrderV3) *okane.Order {
	found, MarketDef := ba.Exchange.Markets.ByName(o.MarketSymbol)
	if !found {
		log.Printf("Failed to look up market '%s' when resolving order.\n", o.MarketSymbol)
		return nil
	}
	if !o.FillQuantity.IsZero() {
	}
	UsdRate := decimal.Zero
	if MarketDef.LastRate == nil {
		log.Errorf("MarketDef.LastRate is nil for '%s'!\n", MarketDef.Name())
	} else {
		UsdRate = MarketDef.LastRate.UsdRate
	}
	Order := okane.Order{
		Account:    ba.Account,
		Uuid:       o.ID,
		Type:       okane.OrderTranslationMapBittrex[o.Type][o.Direction],
		Market:     MarketDef,
		Base:       MarketDef.BaseCoin,
		Quote:      MarketDef.QuoteCoin,
		Bidlimit:   o.Limit,
		Quantity:   o.Quantity,
		Filled:     o.FillQuantity,
		Fee:        o.Commission,
		Created:    o.CreatedAt,
		Closed:     o.ClosedAt,
		TotalPrice: o.FillQuantity.Mul(o.Limit),
		UsdTotal:   UsdRate,
	}
	Order.BaseCoin = Order.Base.Name()
	Order.QuoteCoin = Order.Quote.Name()
	if Order.Type == "" {
		log.Errorf("Failed to map order '%+v' to a string.\n", o)
	}
	return &Order
}

func (ba *BittrexAccount) checkOrderLoop() {
	const STATE_OPEN_DBONLY int = 1    // This is NOT on the open order list but db has it. Notify vanished.
	const STATE_OPEN_NODB int = 2      // This IS open but the db hasn't stored it yet. Notify new.
	const STATE_OPEN_CONFIRMED int = 3 // It was in DB and confirmed still open.
	const STATE_DBOPEN_CLOSED int = 4  // This WAS open in db and we just found it in closed. Notify closed.
	OrderState := make(map[string]int)
	DbOpenOrders, err := ba.Account.GetOpenOrders()
	log.ErrorIff(err, "failed to get open orders from db for %s\n", ba.Identifier())
	for k := range DbOpenOrders.ByUuid {
		OrderState[k] = STATE_OPEN_DBONLY
	}
	orders, err := ba.Handle.GetClosedOrders("all")
	if err != nil {
		log.Errorf("Failed to fetch closed orders for %s from bittrex; reason: '%s'\n", ba.Identifier(), err)
		ba.User.Notices.SendIfDeff("Failed to fetch closed orders for %s from bittrex; reason: '%s'\n", ba.Identifier(), err)
	} else {
		log.Printf("Scanned %d closed orders for '%s'\n", len(orders), ba.Identifier())
		for _, o := range orders {
			//log.Printf("It seems that order id '%s' is NOT present in closed orders.\n", o.ID)
			if okane.OrderTranslationMapBittrex[o.Type][o.Direction] == "" {
				log.Printf("ERROR: Order type '%s', side '%s' has no translation.\n", o.Type, o.Direction)
				continue
			}
			if DbOpenOrders.ByUuid[o.ID] != nil {
				OrderState[o.ID] = STATE_DBOPEN_CLOSED
				Order := ba.BittrexOrderToOkane(&o)
				if Order != nil {
					err = Order.RecordClosedOrder()
					log.ErrorIff(err, "Error closing order %s", Order.Identifier())
					ba.User.Notices.SendIfDeff("%s", Order.FormatOrderClosedChat())
				}
			}
			//		log.Printf("Downloaded: '%+v'\n", Order)
		}
	}
	orders, err = ba.Handle.GetOpenOrders("all")
	if err != nil {
		log.Errorf("Failed to fetch open orders for %s from bittrex; reason: '%s'\n", ba.Identifier(), err)
		ba.User.Notices.SendIfDeff("Failed to fetch open orders for %s from bittrex; reason: '%s'\n", ba.Identifier(), err)
	} else {
		//OpenUUIDs, Fills := okane.LoadOrdersToMaps(OrderQuery)
		for _, o := range orders {
			if OrderState[o.ID] == STATE_OPEN_DBONLY {
				OrderState[o.ID] = STATE_OPEN_CONFIRMED
			} else {
				OrderState[o.ID] = STATE_OPEN_NODB
			}
			//log.Printf("It seems that order id '%s' is NOT present in open orders.\n", o.ID)
			if okane.OrderTranslationMapBittrex[o.Type][o.Direction] == "" {
				log.Printf("ERROR: Order type '%s', side '%s' has no translation.\n", o.Type, o.Direction)
				continue
			}
			Order := ba.BittrexOrderToOkane(&o)
			//log.Printf("UUID '%s' is record id %d\n", Order.Uuid, UUIDs[Order.Uuid])
			if DbOpenOrders.ByUuid[Order.Uuid] != nil {
				dbOrder := DbOpenOrders.ByUuid[Order.Uuid]
				log.Debugf("Confirmed id '%s' is still present in open orders.\n", o.ID)
				if !dbOrder.Filled.Equal(Order.Filled) {
					log.Debugf("Partial fill since the last update of o_order_open!\n")
					ba.OrderUpdatePartialFill(Order)
					dbOrder.Filled = Order.Filled
					log.ErrorIff(ba.Publish(okane.EV_ORDER_UPDATE, Order), "error recording open order for bittrex")
				}
			} else {
				err = Order.RecordOpenOrder()
				log.ErrorIff(err, "failed to record open order: %s\n", Order.Identifier())
				ba.User.Notices.SendIfDeff("%s", Order.FormatOrderOpenChat())
			}
		}
		for uuid := range OrderState {
			if OrderState[uuid] == STATE_OPEN_DBONLY {
				log.Printf("It is time to delete order '%s'; it is not open but is in o_order_open still.\n", uuid)
				err = ba.Account.User.RemoveOpenOrder(uuid)
				log.ErrorIff(err, "failed to remove open order %s\n", uuid)
				ba.Account.User.Notices.SendIfDeff("%s", DbOpenOrders.ByUuid[uuid].FormatOrderVanished())
			}
		}
		log.Printf("Scanned %d open orders for '%s'\n", len(orders), ba.Identifier())
	}
}

func (ba *BittrexAccount) PlaceOrder(o *okane.ActionRequest) error {
	//uuid, err := ba.Handle.BuyLimit(o.Market.Name(), decimal.NewFromFloat(o.Qty), decimal.NewFromFloat(o.Bidlimit))
	var Dir bittrex.OrderDirection
	var Type bittrex.OrderType
	switch o.Command {
	case "CANCEL":
	case "LIMIT_BUY":
		Dir = bittrex.BUY
		Type = bittrex.LIMIT
	case "LIMIT_SELL":
		Dir = bittrex.SELL
		Type = bittrex.LIMIT
	default:
		return fmt.Errorf("Order request %s had unknown action type '%s'\n",
			o.Identifier(), o.Command)
	}
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
	log.Printf("Placing order request %d: '%s %s' on %s for %.08f at %.08f\n",
		o.Id, Type, Dir, o.Market.Name(), o.Qty, o.Bidlimit)
	O := bittrex.CreateOrderParams{
		MarketSymbol: o.Market.Name(),
		Direction:    Dir,
		Type:         Type,
		Quantity:     decimal.NewFromFloat(o.Qty),
		TimeInForce:  "GOOD_TIL_CANCELLED",
		Limit:        o.Bidlimit,
		UseAwards:    "false",
	}
	res, err := ba.Handle.CreateOrder(O)
	if err != nil {
		_, errdb := ba.Account.SetRequest(o, "PLACING", "REJECTED")
		log.ErrorIff(errdb, "Order request %s: failed to record rejection because", o.Identifier())
		return Chaterr.LogErrorf("Placement error on order %s: %s\n",
			o.Identifier(), err)
	}
	o.Uuid = res.ID
	log.Printf("Result: %+v\n", res)
	err = o.MarkAsHandled("PLACED")
	log.ErrorIff(err, "Order request %s: successful order placement",
		o.Identifier())
	return nil
}

func (ba *BittrexAccount) ProcessRequestedOrders() {
	var Cycles int
	for true {
		time.Sleep(time.Duration(5) * time.Second)
		Cycles++
		log.Printf("Order request cycle %d\n", Cycles)
		requests, err := ba.GetOpenOrderRequests()
		if err != nil {
			log.Printf("Account %s: Failed to fetch pending order requests: %s.\n",
				ba.Identifier(), err)
			return
		}
		var taken bool
		for _, Order := range requests {
			taken, err = ba.Account.SetRequest(Order, "REQUESTED", "PLACING")
			if err != nil {
				log.Printf("Account %s error taking order request: %s\n",
					ba.Identifier(), err)
				continue
			}
			if !taken {
				log.Printf("Account %s: Wasn't able to take order_request %d; may have lost election.\n", ba.Identifier(), Order.Id)
				continue
			}
			err = ba.PlaceOrder(Order)
			if err != nil {
				log.Printf("Account %s error processing order request %d: %s\n",
					ba.Identifier(), Order.Id, err)
				taken, err = ba.Account.SetRequest(Order, "PLACING", "REJECTED")
				if err != nil {
					log.Printf("Account %s error marking order request %d rejected: %s\n",
						ba.Identifier(), Order.Id, err)
					continue
				}
			} else {
				log.Printf("Account %s received uuid '%s' back from orderrequest %d\n",
					ba.Identifier(), Order.Uuid, Order.Id)
				err = ba.Account.RecordStratOrder(Order)
				log.ErrorIff(err, "Account %s to record strat order", ba.Identifier())
			}
		}
	}
}

func BittrexUserHandler(Bittrex *BittrexAccount) {
	Id := fmt.Sprintf("BittrexUserHandler(%s)", Bittrex.Identifier())
	Sentry := Sentries.NewSentry(Id)
	log.Printf("Init for user '%s'\n", Bittrex.Identifier())
	Bittrex.Account.User.Events = Global.EventChannel
	go Bittrex.ProcessRequestedOrders()
	for true {
		log.Debugf("Scanning orders for '%s'\n", Bittrex.Identifier())
		Sentry.Checkin("checkOrderLoop()")
		Bittrex.checkOrderLoop()
		Sentry.Checkin("UpdateBalances()")
		Bittrex.UpdateBalances()
		Sentry.Checkin("ProcessRequestedOrders()")
		Sentry.Checkin("Time sleep waiting for next cycle")
		time.Sleep(Global.Timing.Poll)
	}
}

func main() {
	LoadConfigValues("/data/baytor/okane.ini")
	InitAndConnect()
	Chaterr.SendIfDeff("Bittrex central process bootup.\n")
	NoApi := bittrex.New("", "")
	NoApi.SetDebug(false)
	go FetchMarketDefs(NoApi)
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
				BittrexUserHandler(User)
			} else {
				log.Debugf("Skipping spawn of account thread on user '%s'\n", User.Identifier())
			}
		} else {
			go BittrexUserHandler(User)
		}
	}
	for true {
		time.Sleep(time.Hour)
	}
}
