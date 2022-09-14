package main

import (
	"flag"
	"fmt"
	krakenapi "github.com/beldur/kraken-go-api-client"
	_ "github.com/grammaton76/g76golib/chatoutput/sc_dbtable"
	"github.com/grammaton76/g76golib/okane"
	"github.com/grammaton76/g76golib/sentry"
	"github.com/grammaton76/g76golib/shared"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/grammaton76/g76golib/slogger"
	"github.com/shopspring/decimal"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
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

var Global struct {
	db                  *shared.DbHandle
	ChatHandle          *shared.ChatHandle
	Client              *http.Client
	ChatSection         string
	ChatDiags           *shared.ChatTarget
	LocalStorage        string
	MarketDataFile      string
	MarketRatesFile     string
	MarketIntervalsFile string
	PidFile             string
	ExchangeId          int
	Market              *okane.MarketDefsType
	MarketDataCycle     int
	MarketRateCycle     int
	ReadOnly            bool
	UserNames           []string
	Accounts            []*KrakenAccount
	MarketList          []string
	EventChannel        *okane.EventChannel
	Timing              struct {
		OrderCheckInterval time.Duration
		SentryTimeout      time.Duration
	}
	DiagCheckUser string
	NoRateData    bool
}

var rQ struct {
	PullInterval *shared.Stmt
}

type KrakenAccount struct {
	*okane.Account
	Handle  *krakenapi.KrakenApi
	Chat    *shared.ChatTarget
	Section string
}

var CoinLookup shared.LookupTable

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
	if Global.DiagCheckUser != "" {
		Global.NoRateData = true
	}
	Config.LoadAnIni(Ini).OrDie()
	Global.PidFile = Config.GetStringOrDefault("kraken.ratelogpid", "/data/baytor/state/kraken-central.pid", "Defaulting pid file")
	shared.ExitIfPidActive(Global.PidFile)
	Global.ChatSection = Config.GetStringOrDefault("kraken.chathandle", "", "No chat channel defined; no notices will be sent!\n")
	Global.LocalStorage = Config.GetStringOrDie("kraken.localstorage", "")
	Global.MarketDataFile = Config.GetStringOrDefault("kraken.marketdata", Global.LocalStorage+"/marketdata-kraken.json", "")
	Global.MarketRatesFile = Config.GetStringOrDefault("kraken.marketrates", Global.LocalStorage+"/marketrates-kraken.json", "")
	Global.MarketIntervalsFile = Config.GetStringOrDefault("kraken.marketintervals", Global.LocalStorage+"/marketintervals-kraken.json", "")
	found, List := Config.GetString("okane.userlist")
	if found {
		Global.UserNames = strings.Split(List, ",")
	}
	Seconds := Config.GetIntOrDefault("kraken.ordercheck_seconds", 60, "Defaulting to checking orders every 60 seconds.\n")
	Global.Timing.OrderCheckInterval = time.Second * time.Duration(Seconds)
}

func InitAndConnect() {
	Global.Timing.SentryTimeout = time.Minute * 2
	Global.ExchangeId = okane.EXCHANGE_KRAKEN
	Sentries = sentry.NewSentryTeam()
	Sentries.TripwireFunc = func(st *sentry.Sentry) {
		Global.ChatHandle.SendDefaultf("Kraken-central rebooting due to failure on check %d of sentry %s (%s)\n",
			st.Counter(), st.Identifier(), st.Notes())
	}
	Sentries.DefaultTtl = Global.Timing.SentryTimeout
	MapKrknToStd = make(map[string]string)
	MapStdToKrkn = make(map[string]string)
	cookieJar, _ := cookiejar.New(nil)
	Global.Client = &http.Client{
		Jar: cookieJar,
	}
	Global.db = Config.ConnectDbKey("okane.centraldb").OrDie()
	log.FatalIff(okane.DbInit(Global.db), "dbinit failed.\n")
	CoinLookup = shared.NewLookup("coinlookup", Global.db)
	Global.EventChannel = okane.NewEventChannel()
	if Global.ChatSection != "" {
		Global.ChatHandle = Config.NewChatHandle(Global.ChatSection).OrDie("failed to define chat handle")
		Global.ChatDiags = Global.ChatHandle.OutputChannel
	}
	rQ.PullInterval = Global.db.PrepareOrDie("select marketid,max(last),min(last),max(lastusd),min(lastusd) from marketlast where timeat>$1 and exchangeid=$2 group by marketid;")
	// Top level section lists out the sections where the keys are defined.
	for _, Section := range Global.UserNames {
		Username := Config.GetStringOrDie(Section+".username", "Username is required")
		log.Printf("Loading config for user '%s'\n", Section)
		found, sList := Config.GetString(Section + ".kraken")
		if !found {
			log.Printf("No kraken handles for user '%s'\n", Section)
		}
		UserDb := Config.ConnectDbKey(Section + ".database").OrDie()
		if UserDb.IsDead() {
			log.Printf("Cannot connect to db for '%s', skipping this user.\n", Section)
			continue
		}
		Lists := strings.Split(sList, ",")
		User := okane.NewUser(UserDb)
		User.Username = Username
		// Underneath the user are accounts, not users.
		for _, UserSection := range Lists {
			log.Printf("Scanning accounts for section '%s'; now doing '%s'\n", Section, UserSection)
			Accounts := User.LoadAccountsForExchange(okane.Exchanges.ById[okane.EXCHANGE_KRAKEN])
			if len(Accounts) == 0 {
				log.Infof("Just FYI, there are no accounts in the db for section '%s'\n", Section)
			}
			for _, Account := range Accounts {
				bAccount := ToExchangeAccount(Account, Section)
				Global.Accounts = append(Global.Accounts, bAccount)
			}
		}
	}
	Global.EventChannel = okane.NewEventChannel()
	Global.EventChannel.Connect()
}

var MapKrknToStd map[string]string
var MapStdToKrkn map[string]string

func (ExchangeAcct *KrakenAccount) GetTicker() ([]okane.Order, error) {
	return nil, nil
}

func FetchMarketTickers() {
	var Cycle int
	OldMarkets := okane.NewMarketDef()
	Sentry := Sentries.NewSentry("FetchMarketTickers")
	MarketRates := make(okane.MarketRatesType)
	StoreCoins := make(map[string]int)
	StoreCoins["XXBT"] = 1
	StoreCoins["XBT"] = 1
	StoreCoins["BTC"] = 1
	for true {
		Cycle++
		var Queries []string
		NewMarkets := okane.NewMarketDef()
		Queries = append(Queries, "XETHZUSD,XXBTZUSD,ZEURZUSD")
		Sentry.Checkin("market scan, cycle %d", Cycle)
		//UpdateTime := time.Now()
		Url := "https://api.kraken.com/0/public/AssetPairs"
		resp, err := Global.Client.Get(Url)
		log.ErrorIff(err, "Couldn't perform query to pull market tickers.\n")
		if resp.StatusCode != 200 {
			log.Fatalf("HTTP %d result from GET %s\n", resp.StatusCode, Url)
		}
		Return, err := ioutil.ReadAll(resp.Body)
		Pairs := sjson.NewJson()
		Pairs.IngestFromBytes(Return)
		Results := Pairs["result"].(map[string]interface{})
		//		log.FatalIff(err, "Failed to download market summaries.\n")

		for Name, iMarketdef := range Results {
			Marketdef := sjson.NewJson()
			Marketdef.IngestFromObject(iMarketdef)
			Base := Marketdef.KeyString("base")
			Quote := Marketdef.KeyString("quote")
			Market := okane.MarketDef{
				BaseCoin:  okane.CoinLookup.ByNameOrAdd(Base),
				QuoteCoin: okane.CoinLookup.ByNameOrAdd(Quote),
				//Status:       v.Status,
				//Created:      v.CreatedAt,
				UsProhibited: false,
				Exchange:     okane.Exchanges.ById[okane.EXCHANGE_KRAKEN],
				//Notice:       v.Notice,
				//Precision:    decimal.NewFromInt(1).Shift(-v.Precision),
			}
			if OldMarkets[Name] != nil {
				Market.LastRate = OldMarkets[Name].LastRate
			}
			Market.SetSymbol(Name)
			log.Debugf("Market %s: base %s, quote %s\n", Name, Base, Quote)
			NewMarkets[Name] = &Market
		}
		{
			var Buffer []string
			for MarketName, Marketdef := range NewMarkets {
				if !(StoreCoins[Marketdef.BaseCoin.Name()] == 1) &&
					!(StoreCoins[Marketdef.QuoteCoin.Name()] == 1) {
					continue
				}
				Buffer = append(Buffer, MarketName)
				if len(Buffer) == 10 {
					Queries = append(Queries, strings.Join(Buffer, ","))
					Buffer = nil
				}
			}
			Queries = append(Queries, strings.Join(Buffer, ","))
		}
		log.Printf("Performing %d fetches to get all of the markets we care about.\n",
			len(Queries))
		for _, Query := range Queries {
			Url := fmt.Sprintf("https://api.kraken.com/0/public/Ticker?info=margin&pair=%s", Query)
			resp, err := Global.Client.Get(Url)
			log.ErrorIff(err, "Couldn't perform query to pull market tickers.\n")
			if resp.StatusCode != 200 {
				log.Fatalf("HTTP %d result from GET %s\n", resp.StatusCode, Url)
			}
			Return, err := ioutil.ReadAll(resp.Body)
			Markets := sjson.NewJson()
			{
				RateReturn := sjson.NewJson()
				RateReturn.IngestFromBytes(Return)
				Results := RateReturn["result"]
				Markets.IngestFromObject(Results)
			}
			GetVal := func(Key string, i int, MarketData interface{}) decimal.Decimal {
				Caw := MarketData.(map[string]interface{})[Key]
				Val, _ := decimal.NewFromString(Caw.([]interface{})[i].(string))
				//log.Printf("Key %s has last %s\n", Key, Val.StringFixed(8))
				return Val
			}
			for MarketName, MarketData := range Markets {
				_, Market := NewMarkets.ByName(MarketName)
				Quote := okane.MarketQuote{
					MarketDef: Market,
					Last:      GetVal("c", 0, MarketData),
					Bid:       GetVal("b", 0, MarketData),
					Ask:       GetVal("a", 0, MarketData),
					UsdRate:   decimal.Decimal{},
					Volume: decimal.NullDecimal{
						Decimal: GetVal("v", 1, MarketData),
						Valid:   true,
					},
					LastUpdate: time.Now(),
				}
				Market.LastRate = &Quote
				MarketRates[MarketName] = &Quote
				//				log.Printf("Market %s: last %s, bid %s, ask %s\n",
				//					MarketName, Market.LastRate.Last.StringFixed(8), Market.LastRate.Bid.StringFixed(8), Market.LastRate.Ask.StringFixed(8))
			}
		}

		MarketRates.SetFiat(&okane.FiatMap{
			BtcMarket:  MarketRates["XXBTZUSD"],
			EthMarket:  MarketRates["XETHZUSD"],
			EuroMarket: MarketRates["ZEURZUSD"],
		})
		if NewMarkets.Equals(&OldMarkets) {
			log.Printf("Market data in '%s' has had no changes\n", Global.MarketDataFile)
		} else {
			Bob := sjson.NewJsonFromObject(&NewMarkets)
			log.ErrorIff(Bob.WriteToFile(Global.MarketDataFile), "writing out json file '%s'", Global.MarketDataFile)
			log.Printf("Writing market data to '%s' due to changes\n", Global.MarketDataFile)
		}
		Bob := sjson.NewJsonFromObject(&MarketRates)
		log.ErrorIff(Bob.WriteToFile(Global.MarketRatesFile), "writing out json file '%s'", Global.MarketRatesFile)
		MarketRates.WriteMarketLast(Global.db, okane.Exchanges.ById[okane.EXCHANGE_KRAKEN])
		log.Printf("Writing market rates to '%s' due to changes\n", Global.MarketRatesFile)
		Sentry.Checkin("waiting for cycle %d", Cycle+1)
		Global.Market = &NewMarkets
		Global.MarketRateCycle++
		time.Sleep(time.Minute)
		OldMarkets = NewMarkets
	}
}

func FetchMarketIntervals() {
	const Hour int = 3600
	var Intervals []int = []int{Hour, 3 * Hour, 6 * Hour, 12 * Hour, 24 * Hour, 48 * Hour, 72 * Hour, 24 * 7 * Hour}
	LastRuns := make([]time.Time, len(Intervals))
	var Cycles int
	IntervalData := make(map[int]map[string]*okane.MarketRange)
	ShortestInterval := Intervals[0]
	Sentry := Sentries.NewSentry("FetchMarketIntervals")
	log.Printf("Don't forget to put the intervals into a database record.\n")
	for true {
		Cycles++
		log.Printf("Running interval data, cycle %d.\n", Cycles)
		Now := time.Now()
		var RangeUpdates int
		for k, Interval := range Intervals {
			Sentry.Checkin("scanning interval %d, cycle %d",
				Interval, Cycles)
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
			//log.Printf("Stats for '%s' - '%s'\n", (time.Duration(Interval) * time.Second).String(), Start)
			res, err := rQ.PullInterval.Query(Start, Global.ExchangeId)
			log.FatalIff(err, "Error pulling interval rates for '%d' with '%s'\n", Interval, Start)
			Results := make(map[string]*okane.MarketRange)
			ResultsUS := make(map[string]*okane.MarketRange)
			var Count int
			var DeadMarkets []string
			for res.Next() {
				err = res.Scan(&marketid, &max, &min, &maxusd, &minusd)
				log.FatalIff(err, "Caw, error")
				MarketItem := okane.MarketLookup.ById(marketid)
				if MarketItem == nil {
					log.Printf("Not touching unknown market id %d.\n", marketid)
					continue
				}
				found, MarketDef := Global.Market.ByName(MarketItem.Name())
				if !found {
					//log.Debugf("Skipping interval data for non-tracked (or removed) market '%s'\n", MarketItem.Name())
					continue
				}
				Rec := okane.MarketRange{
					MarketDef:  MarketDef,
					MinRate:    decimal.NewFromFloat(min),
					MinRateUsd: decimal.NewFromFloat(minusd),
					MaxRate:    decimal.NewFromFloat(max),
					MaxRateUsd: decimal.NewFromFloat(maxusd),
				}
				Name := MarketDef.Name()
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
			//log.Printf(" %d records retrieved.\n", Count)
			LastRuns[k] = Now
		}
		//os.Exit(3)
		Bob := sjson.NewJsonFromObject(&IntervalData)
		log.ErrorIff(Bob.WriteToFile(Global.MarketIntervalsFile), "writing out json file '%s'", Global.MarketIntervalsFile)
		//EventChannel.WriteJson(Bob)
		time.Sleep(time.Minute)
	}
}

func ToExchangeAccount(acct *okane.Account, Section string) *KrakenAccount {
	var err error
	AuthSection := Config.GetStringOrDie(Section+".kraken", "Must have a list of kraken accounts (even if empty)")
	Bob := &KrakenAccount{
		Account: acct,
		Section: Section,
	}
	Bob.Exchange = okane.Exchanges.ById[okane.EXCHANGE_KRAKEN]
	Key := Config.GetStringOrDie(AuthSection+".key", "Key is required.\n")
	Secret := Config.GetStringOrDie(AuthSection+".secret", "Secret is required.\n")
	Bob.Handle = krakenapi.New(Key, Secret)
	log.ErrorIff(err, "")
	return Bob
}

func (ExchangeAcct *KrakenAccount) GetBalances() ([]okane.CoinBalance, error) {
	Native, err := ExchangeAcct.Handle.Balance()
	log.FatalIff(err, "Failed to get balances.\n")
	var Results []okane.CoinBalance
	Data := sjson.NewJsonFromObject(&Native)
	for k := range *Data {
		//Coin := KrakenToStd(k)
		Coin := okane.CoinLookup.ByNameOrAdd(k)
		if Coin == nil || Coin.IsNil() {
			continue
		}
		Balance, _ := decimal.NewFromString(Data.KeyString(k))
		if Balance.IsZero() {
			continue
		}
		log.Printf("%s (%s): %s\n", k, Coin.Name(), Balance.String())
		Rec := okane.CoinBalance{
			Coin:      Coin,
			Balance:   Balance,
			Available: decimal.Decimal{},
			Hold:      decimal.Decimal{},
		}
		Results = append(Results, Rec)
	}
	return Results, nil
}

func (ExchangeAcct *KrakenAccount) UpdateBalances() {
	StakeCheckInterval := time.Hour
	var NextStakeCheck time.Time
	StakingBalances := make(map[int]decimal.Decimal)
	Sentry := Sentries.NewSentry("balances-%s", ExchangeAcct.Identifier())
	var Cycles int
	for true {
		//log.Printf("%s\n", Check)
		Cycles++
		Sentry.Checkin("cycle %d", Cycles)
		Times := make(map[string]*time.Time)
		if NextStakeCheck.Before(time.Now()) {
			LedgerEntries, err := ExchangeAcct.Handle.Ledgers(nil)
			log.FatalIff(err, "Failed downloading ledgers")
			log.Printf("Running stake check for %s.\n", ExchangeAcct.Identifier())
			StakingBalances = make(map[int]decimal.Decimal)
			for _, Entry := range LedgerEntries.Ledger {
				if Entry.Type == "staking" {
					Time := time.Unix(int64(Entry.Time), 0)
					Balance, _ := decimal.NewFromString(Entry.Balance.String())
					if Times[Entry.Asset] != nil && Time.Before(*Times[Entry.Asset]) {
						log.Debugf("Suppressed staking balance '%s' at '%s' is '%s', after amount '%s' and fee '%s'\n",
							Entry.Asset, Time, Entry.Balance.String(), Entry.Amount.String(), Entry.Fee.String())
						continue
					}
					log.Printf("Latest staking balance '%s' at '%s' is '%s', after amount '%s' and fee '%s'\n",
						Entry.Asset, Time, Entry.Balance.String(), Entry.Amount.String(), Entry.Fee.String())
					Times[Entry.Asset] = &Time
					Coin := okane.CoinLookup.ByNameOrAdd(Entry.Asset)
					StakingBalances[Coin.Id()] = Balance
				}
			}
			NextStakeCheck = time.Now().Add(StakeCheckInterval)
			log.Printf("Next staking check for '%s' set for '%s'\n", ExchangeAcct.Identifier(), NextStakeCheck.String())
		}

		log.Printf("Pulling conventional balances for %s\n", ExchangeAcct.Identifier())
		Data, err := ExchangeAcct.GetBalances()
		log.FatalIff(err, "Failed to get balances on %s\n", ExchangeAcct.Identifier())
		Minimum, _ := decimal.NewFromString("0.00000001")
		Bal := ExchangeAcct.NewBalanceSnapshot()
		for k, v := range StakingBalances {
			Bal.Add(k, v, decimal.Zero, v)
		}
		for _, v := range Data {
			if v.Balance.LessThanOrEqual(Minimum) {
				continue
			}
			Bal.Add(v.Coin.Id(), v.Balance, v.Available, v.Hold)
		}
		log.ErrorIff(Bal.UpdateDb(), "transaction error on balance update for kraken")
		Sentry.Checkin("waiting to start cycle %d", Cycles+1)
		time.Sleep(time.Minute)
	}
}

func (ExchangeAcct *KrakenAccount) KrakenOrderToOkane(o *sjson.JSON) *okane.Order {
	log.Fatalf("Stubbed function not defined.\n")
	MarketName := o.KeyString("symbol")
	MarketDef := Global.Market.ByNameOrDie(MarketName)
	Fill := o.KeyDecimal("fill")
	if !Fill.IsZero() {
	}
	UsdRate := decimal.Zero
	if MarketDef.LastRate == nil {
		log.Errorf("MarketDef.LastRate is nil for '%s'!\n", MarketDef.Name())
	} else {
		UsdRate = MarketDef.LastRate.UsdRate
	}
	Uuid := o.KeyString("uuid")
	Type := o.KeyString("type")
	Direction := o.KeyString("direction")
	Limit := o.KeyDecimal("limit")
	Quantity := o.KeyDecimal("quantity")
	FillQuantity := o.KeyDecimal("filled")
	Commission := o.KeyDecimal("fee")
	sClosedAt := o.KeyString("closed")
	ClosedAt, _ := shared.ParseMysqlTime(sClosedAt)
	sCreatedAt := o.KeyString("opened")
	CreatedAt, _ := shared.ParseMysqlTime(sCreatedAt)
	Order := okane.Order{
		Account:    ExchangeAcct.Account,
		Uuid:       Uuid,
		Type:       okane.OrderTranslationMapKraken[Type][Direction],
		Market:     MarketDef,
		Base:       MarketDef.BaseCoin,
		Quote:      MarketDef.QuoteCoin,
		Bidlimit:   Limit,
		Quantity:   Quantity,
		Filled:     FillQuantity,
		Fee:        Commission,
		Created:    *CreatedAt,
		Closed:     *ClosedAt,
		TotalPrice: FillQuantity.Mul(Limit),
		UsdTotal:   UsdRate,
	}
	Order.BaseCoin = Order.Base.Name()
	Order.QuoteCoin = Order.Quote.Name()
	if Order.Type == "" {
		log.Errorf("Failed to map order '%+v' to a string.\n", o)
	}
	return &Order
}

func (ExchangeAcct *KrakenAccount) checkOrderLoop() {
	const STATE_OPEN_DBONLY int = 1    // This is NOT on the open order list but db has it. Notify vanished.
	const STATE_OPEN_NODB int = 2      // This IS open but the db hasn't stored it yet. Notify new.
	const STATE_OPEN_CONFIRMED int = 3 // It was in DB and confirmed still open.
	const STATE_DBOPEN_CLOSED int = 4  // This WAS open in db and we just found it in closed. Notify closed.
	OrderState := make(map[string]int)
	log.Fatalf("Stubbed function not defined.\n")
	orders, err := ExchangeAcct.GetClosedOrders("all")
	log.Printf("Scanned %d closed orders for '%s'\n", len(orders), ExchangeAcct.Identifier())
	for _, o := range orders {
		o.RecordClosedOrder()
		//		log.Printf("Downloaded: '%+v'\n", Order)
	}
	orders, err = ExchangeAcct.GetOpenOrders("all")
	log.FatalIff(err, "Failed to fetch orders on '%s'.\n", ExchangeAcct.Identifier())

	DbOpenOrders, err := ExchangeAcct.Account.GetOpenOrders()
	log.ErrorIff(err, "failed to get open orders from db for %s\n", ExchangeAcct.Identifier())
	for k := range DbOpenOrders.ByUuid {
		OrderState[k] = STATE_OPEN_DBONLY
	}
	for _, Order := range orders {
		Uuid := Order.Uuid
		if OrderState[Uuid] == STATE_OPEN_DBONLY {
			OrderState[Uuid] = STATE_OPEN_CONFIRMED
		} else {
			OrderState[Uuid] = STATE_OPEN_NODB
		}
		DbOrder := DbOpenOrders.ByUuid[Order.Uuid]
		if DbOrder != nil {
			log.Debugf("Confirmed id '%s' is still present in open orders.\n", Order.Uuid)
			if !DbOrder.Filled.Equals(Order.Filled) {
				log.Debugf("Partial fill since the last update of o_order_open!\n")
				ExchangeAcct.OrderUpdatePartialFill(Order)
				DbOrder.Filled = Order.Filled
				log.ErrorIff(ExchangeAcct.Publish(okane.EV_ORDER_UPDATE, Order), "error recording open order for bittrex")
			}
		} else {
			err = Order.RecordOpenOrder()
			log.ErrorIff(err, "failed to record open order: %s\n", Order.Identifier())
			ExchangeAcct.User.Notices.SendfIfDef("%s", Order.FormatOrderOpenChat())
		}
	}
	for uuid := range OrderState {
		if OrderState[uuid] == STATE_OPEN_DBONLY {
			log.Printf("It is time to delete order '%s'; it is not open but is in o_order_open still.\n", uuid)
			err = ExchangeAcct.Account.User.RemoveOpenOrder(uuid)
			log.ErrorIff(err, "failed to remove open order %s\n", uuid)
			ExchangeAcct.User.Notices.SendfIfDef("%s", DbOpenOrders.ByUuid[uuid].FormatOrderVanished())
		}
	}
	log.Printf("Scanned %d open orders for '%s'\n", len(orders), ExchangeAcct.Identifier())
}

func ExchangeUserHandler(Kraken *KrakenAccount) {
	log.Printf("User handler for user '%s'\n", Kraken.Identifier())
	Kraken.Account.User.Events = Global.EventChannel
	go Kraken.UpdateBalances()
	for true {
		//log.Debugf("Scanning orders for '%s'\n", Kraken.Identifier())
		//Kraken.checkOrderLoop()
		time.Sleep(Global.Timing.OrderCheckInterval)
	}
}

func (ExchangeAcct *KrakenAccount) GetClosedOrders(market string) ([]*okane.Order, error) {
	log.Fatalf("Stubbed function not defined.\n")
	return nil, nil
}

func (ExchangeAcct *KrakenAccount) GetOpenOrders(market string) ([]*okane.Order, error) {
	log.Fatalf("Stubbed function not defined.\n")
	return nil, nil
}

func main() {
	LoadConfigValues("/data/baytor/okane.ini")
	InitAndConnect()
	Global.ChatDiags.SendfIfDef("Kraken central process bootup.\n")
	if !Global.NoRateData {
		go FetchMarketTickers()
	}
	for Global.MarketRateCycle == 0 {
		time.Sleep(time.Second)
	}
	if !Global.NoRateData {
		go FetchMarketIntervals()
	}
	log.Printf("Market definitions loaded.\n")
	for _, User := range Global.Accounts {
		if Global.DiagCheckUser != "" {
			if User.Section == Global.DiagCheckUser {
				ExchangeUserHandler(User)
			} else {
				log.Debugf("Skipping spawn of account thread on user '%s'\n", User.Identifier())
			}
		} else {
			go ExchangeUserHandler(User)
		}
	}
	for true {
		time.Sleep(time.Hour)
	}
}
