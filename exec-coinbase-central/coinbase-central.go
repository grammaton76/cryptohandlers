package main

/* Dev grants
grant update,select,update on all sequences in schema public to localdev;
GRANT SELECT,insert,update,delete ON ALL TABLES IN SCHEMA public TO localdev;
*/

import (
	"flag"
	"fmt"
	_ "github.com/grammaton76/g76golib/chatoutput/sc_dbtable"
	"github.com/grammaton76/g76golib/okane"
	"github.com/grammaton76/g76golib/sentry"
	"github.com/grammaton76/g76golib/shared"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/grammaton76/g76golib/slogger"
	coinbasepro "github.com/preichenberger/go-coinbasepro/v2"
	"github.com/shopspring/decimal"
	"net/http"
	"os"
	"strings"
	"time"
)

var log slogger.Logger

var Sentries *sentry.SentryTeam

var Config shared.Configuration

var Cli struct {
	Inifile  string
	ReadOnly bool
	Debug    bool
	Test     string
}

type ListOfCoinbaseAccounts []*CoinbaseAccount

var Global struct {
	CentralRw   *shared.DbHandle
	Accounts    ListOfCoinbaseAccounts
	readclient  *coinbasepro.Client
	ChatSection string
	UserNames   []string
	ChatHandle  *shared.ChatHandle
	ReadOnly    bool
	Timing      struct {
		Poll    time.Duration
		Timeout time.Duration
	}
	Pidfile             string
	User                *okane.User
	LocalStorage        string
	MarketDataFile      string
	MarketRatesFile     string
	MarketIntervalsFile string
	Market              *okane.MarketDefsType
	MarketDataCycle     int
	MarketRateCycle     int
	EventChannel        *okane.EventChannel
	LoadProgress        string
}

var rQ struct {
	PullInterval *shared.Stmt
}

type CoinbaseAccount struct {
	*okane.Account
	Handle  *coinbasepro.Client
	Notices *shared.ChatTarget
	Section string
}

var Coinbase = okane.Exchanges.ById[okane.EXCHANGE_COINBASE]

func (loa ListOfCoinbaseAccounts) List() string {
	var Caw []string
	for _, v := range loa {
		Caw = append(Caw, v.Identifier())
	}
	return strings.Join(Caw, ", ")
}

func ToCoinbaseAccount(acct *okane.Account, Section string) *CoinbaseAccount {
	AuthSection := Config.GetStringOrDie(Section+".coinbase", "Must have a list of coinbase accounts (even if empty)")
	Bob := &CoinbaseAccount{
		Account: acct,
		Section: Section,
	}
	key := Config.GetStringOrDie(AuthSection+".key", "ERROR: No coinbase key found in ini.")
	secret := Config.GetStringOrDie(AuthSection+".secret", "ERROR: No coinbase secret found in ini.")
	passphrase := Config.GetStringOrDie(AuthSection+".passphrase", "ERROR: No coinbase passphrase found in ini.")
	client := coinbasepro.NewClient()
	client.UpdateConfig(&coinbasepro.ClientConfig{
		BaseURL:    "https://api.exchange.coinbase.com",
		Key:        key,
		Passphrase: passphrase,
		Secret:     secret,
	})
	if client == nil {
		log.Printf("Nil client from section '%s'\n", Section)
	}
	client.HTTPClient = &http.Client{Timeout: time.Second * 15}
	Bob.Handle = client
	if Global.readclient == nil {
		Global.readclient = client
	}
	//Return error here if fail
	return Bob
}

func updateBalanceLoop() {
	if len(Global.Accounts) == 0 {
		log.Printf("No accounts to scan; skipping balance-check loop.\n")
		return
	}
	var Count int
	for true {
		Count++
		for _, AccountHandle := range Global.Accounts {
			Id := fmt.Sprintf("balance-%s (%s)", AccountHandle.Identifier(), AccountHandle.Exchange.Name)
			Sentry := Sentries.CheckEnsure(Id, "scanning balances, pass %d", Count)
			switch AccountHandle.Exchange.Name {
			case "coinbase":
				var Balances []*okane.CoinBalance
				Balances = AccountHandle.getCbBalance()
				Sentry.Update("reconciling balances in db - pass %d", Count)
				Snapshot := AccountHandle.NewBalanceSnapshot()
				for _, b := range Balances {
					Snapshot.Add(b.Coin.Id(), b.Balance, b.Available, b.Hold)
				}
				Sentry.Checkin("coinbase getAccounts() - pass %d", Count)
				accounts, err := AccountHandle.Handle.GetAccounts()
				log.FatalIff(err, "ERROR in updateBalanceLoop")
				Sentry.Checkin("start transaction")
				for _, a := range accounts {
					Sentry.Checkin(fmt.Sprintf("pulling balance for %s (%s)",
						a.Currency, a.ID))
					var CoinId int
					Balance, _ := decimal.NewFromString(a.Balance)
					Hold, _ := decimal.NewFromString(a.Hold)
					Available, _ := decimal.NewFromString(a.Available)
					if Balance.IsZero() && Hold.IsZero() {
						continue
					}
					CoinId = okane.CoinLookup.LabelToId(a.Currency, true).Id()
					Snapshot.Add(CoinId, Balance, Available, Hold)
					Sentry.Update(fmt.Sprintf("updating balance for account %s\n",
						a.ID))
				}
				err = Snapshot.UpdateDb()
				log.ErrorIff(err, "writing Coinbase balance snapshot to database")
			default:
				log.Fatalf("...there is no good answer for that, is there?\n")
			}
			log.Printf("Account '%s' balance checks completed.\n", AccountHandle.Identifier())

		}
		for _, AccountHandle := range Global.Accounts {
			Id := fmt.Sprintf("balance-%s", AccountHandle.Identifier())
			Sentries.CheckEnsure(Id, "waiting for next run")
		}
		time.Sleep(Global.Timing.Poll)
	}
}

// TODO: Exponential backoff should be a thing.
func checkOrderLoop() {
	const STATE_OPEN_DBONLY int = 1    // This is NOT on the open order list but db has it. Notify vanished.
	const STATE_OPEN_NODB int = 2      // This IS open but the db hasn't stored it yet. Notify new.
	const STATE_OPEN_CONFIRMED int = 3 // It was in DB and confirmed still open.
	const STATE_DBOPEN_CLOSED int = 4  // This WAS open in db and we just found it in closed. Notify closed.
	var Count int
	log.Printf("Async order-check thread started; monitoring accounts: %s\n",
		Global.Accounts.List())
	for true {
		Count++
		for _, AccountHandle := range Global.Accounts {
			var DbOpenOrders *okane.OrderList
			var err error
			cbclient := AccountHandle.Handle
			OrderState := make(map[string]int)
			Id := fmt.Sprintf("checkorder-%s (%s)",
				AccountHandle.Identifier(), AccountHandle.Exchange.Name)
			Sentry := Sentries.CheckEnsure(Id, "Querying db for open orders on credential %d, pass %d",
				AccountHandle.CredId, Count)
			DbOpenOrders, err = AccountHandle.GetOpenOrders()
			log.FatalIff(err, "failed OrderOpenForCred(%d)", AccountHandle.CredId)
			Sentry.Checkin("Loading orders to maps, pass %d", Count)
			for k := range DbOpenOrders.ByUuid {
				OrderState[k] = STATE_OPEN_DBONLY
			}

			Phases := []string{"open", "closed"}
			for _, Phase := range Phases {
				log.Printf("Listing orders via API, pass %d phase %s", Count, Phase)
				Sentry.Checkin("Listing orders via API, pass %d phase %s", Count, Phase)

				var cursor *coinbasepro.Cursor
				switch Phase {
				case "open":
					cursor = cbclient.ListOrders(coinbasepro.ListOrdersParams{Status: "open"})
					var Pages int
					for cursor.HasMore {
						Pages++
						Sentry.Checkin("Fetching page %d of orders %s", Pages, Phase)
						var orders []coinbasepro.Order
						if err := cursor.NextPage(&orders); err != nil {
							log.Printf("Failed pulling orders: %s!\n", err)
							continue
						}
						log.Debugf("There are %d %s orders for %s\n", len(orders), Phase, AccountHandle.Identifier())
						for _, o := range orders {
							if o.ID != "" {
								if OrderState[o.ID] == STATE_OPEN_DBONLY {
									OrderState[o.ID] = STATE_OPEN_CONFIRMED
								} else {
									OrderState[o.ID] = STATE_OPEN_NODB
								}
							}
							OrderToLog := NativeOrderToOkane(AccountHandle, &o)
							if DbOpenOrders.ByUuid[o.ID] != nil {
								log.Debugf("Confirmed id '%s' is still present in open orders.\n", o.ID)
								Fill, _ := decimal.NewFromString(o.FilledSize)
								if !DbOpenOrders.ByUuid[o.ID].Filled.Equals(Fill) {
									log.Debugf("Partial fill since the last update of o_order_open!\n")
									AccountHandle.OrderUpdatePartialFill(OrderToLog)
								}
							} else {
								log.Printf("It seems that order id '%s' is NOT present in open orders.\n", o.ID)
								if okane.OrderTranslationMapCbpro[o.Type][o.Side] == "" {
									log.Printf("ERROR: Order type '%s', side '%s' has no translation.\n", o.Type, o.Side)
									continue
								}
								err = OrderToLog.RecordOpenOrder()
								log.ErrorIff(err, "failed to record open order: %s\n", OrderToLog.Identifier())
								AccountHandle.User.Notices.SendfIfDef("%s", OrderToLog.FormatOrderOpenChat())
							}
						}
					}
				case "closed":
					var ledgers []coinbasepro.LedgerEntry
					accounts, err := cbclient.GetAccounts()
					if err != nil {
						println(err.Error())
					}
					Sentry.Checkin("Listing orders via API, pass %d phase %s Ledgers", Count, Phase)
					AccountMarkets := make(map[string]map[string]int)
					log.Printf("Pulling ledgers for %s (%d sub accounts)\n", AccountHandle.Identifier(), len(accounts))
					for _, cbsubacct := range accounts {
						var Trades int
						var Unrecorded int
						Coin := cbsubacct.Currency
						cursor := cbclient.ListAccountLedger(cbsubacct.ID)
						for cursor.HasMore {
							err = cursor.NextPage(&ledgers)
							if err != nil {
								log.ErrorIff(err, "failed fetching %s ledgers", Coin)
								time.Sleep(time.Second / 2)
								continue
							} else {
								time.Sleep(time.Second / 10)
							}
							for _, e := range ledgers {
								if e.Type != "match" || e.Details.OrderID == "" {
									continue
								}
								Trades++
								if DbOpenOrders.ByUuid[e.Details.OrderID] != nil {
									OrderState[e.Details.OrderID] = STATE_DBOPEN_CLOSED
								}
								if AccountHandle.User.UuidCloseRecorded(e.Details.OrderID) {
									continue
								}
								Unrecorded++
								if AccountMarkets[e.Details.ProductID] == nil {
									AccountMarkets[e.Details.ProductID] = make(map[string]int)
								}

								log.Debugf("%s %s Ledger: ID %s Type %s Balance %s Amount %s Created %s - order %s, product %s, tradeid %s\n",
									AccountHandle.Identifier(), Coin, e.ID, e.Type, e.Balance, e.Amount, e.CreatedAt.Time().String(),
									e.Details.OrderID, e.Details.ProductID, e.Details.TradeID)
								AccountMarkets[e.Details.ProductID][e.Details.OrderID]++
							}
						}
						log.Debugf("Scanning %s %-8s ledger for %s yielded %d records (%d new)\n",
							Phase, cbsubacct.Currency, AccountHandle.Identifier(), Trades, Unrecorded)
					}
					log.Printf("Ledgers pulled; unclosed orders detected: %+v\n", AccountMarkets)
					for ProductName, Unclosed := range AccountMarkets {
						if len(Unclosed) > 0 {
							log.Printf("Pulling market %s history - found %d UUIDs with no close record.\n",
								ProductName, len(Unclosed))
						}
						for Uuid := range Unclosed {
							//log.Printf("Performing search for %s\n", Uuid)
							Sentry.Checkin("Pulling historical order records on cycle %d phase %s - %s", Count, Phase, Uuid)
							cursor := cbclient.ListFills(coinbasepro.ListFillsParams{
								OrderID: Uuid, Pagination: coinbasepro.PaginationParams{Limit: 25}})
							for cursor.HasMore {
								var orders []coinbasepro.Order
								err := cursor.NextPage(&orders)
								if err != nil {
									log.Errorf("Error scanning orders on bucket for phase %s on market %s: %s\n", Phase, ProductName, err)
									time.Sleep(time.Second / 2)
									continue
								} else {
									time.Sleep(time.Second / 10)
								}
								for _, v := range orders {
									//log.Printf("Assigning uuid %s into record...\n", Uuid)
									v.ID = Uuid
									OrderToLog := NativeOrderToOkane(AccountHandle, &v)
									OrderToLog.Closed = OrderToLog.Created
									err = OrderToLog.RecordClosedOrder()
									log.ErrorIff(err, "Error closing order %s", OrderToLog.Identifier())
									AccountHandle.User.Notices.SendfIfDef("%s", OrderToLog.FormatOrderClosedChat())
								}
							}
						}
					}
					Sentry.Checkin("Listing orders via API, pass %d phase %s ListFills", Count, Phase)
				}
				for uuid, State := range OrderState {
					switch State {
					case STATE_OPEN_DBONLY:
						// Notify vanished.
						log.Printf("It is time to delete order '%s'; it is not open but is in o_order_open still.\n", uuid)
						err = AccountHandle.Account.User.RemoveOpenOrder(uuid)
						log.ErrorIff(err, "failed to remove open order %s\n", uuid)
						AccountHandle.User.Notices.SendfIfDef("%s", DbOpenOrders.ByUuid[uuid].FormatOrderVanished())
					case STATE_OPEN_NODB:
						// Notify new - handled where we call RecordOpenOrder()
					case STATE_OPEN_CONFIRMED:
						// Nothing new has happened, no notify.
					case STATE_DBOPEN_CLOSED:
						// Notify completion - handled where we call FormatOrderClosedChat()
					default:
						log.Errorf("Unknown OrderState %d encountered on order '%s'!\n", OrderState[uuid], uuid)
					}
				}
			}
			log.Printf("Order check cycle %d completed; waiting %s.\n", Count, Global.Timing.Poll.String())
			for _, AccountHandle := range Global.Accounts {
				Id := fmt.Sprintf("checkorder-%s", AccountHandle.Identifier())
				Sentries.CheckEnsure(Id, "waiting through timeout")
			}
			time.Sleep(Global.Timing.Poll)
		}
	}
}

// TODO: This should iterate over a list of accounts.
// TODO: Exponential backoff should be a thing.
func placeOrderLoop() {
	log.Printf("Async order-placing thread started.\n")
	for true {
		log.Printf("Empty order-placing loop completed.\n")
		time.Sleep(Global.Timing.Poll)
	}
}

func NativeOrderToOkane(Account *CoinbaseAccount, o *coinbasepro.Order) *okane.Order {
	if o.ID == "" {
		return nil
	}
	Market := Global.Market.ByNameOrDie(o.ProductID)
	OrderToLog := okane.Order{
		Account: Account.Account,
		Uuid:    o.ID,
		Type:    okane.OrderTranslationMapCbpro[o.Type][o.Side],
		Market:  Market,
		Base:    Market.BaseCoin,
		Quote:   Market.QuoteCoin,
		Created: o.CreatedAt.Time(),
	}
	OrderToLog.Filled, _ = decimal.NewFromString(o.FilledSize)
	OrderToLog.Bidlimit, _ = decimal.NewFromString(o.Price)
	OrderToLog.Quantity, _ = decimal.NewFromString(o.Size)
	OrderToLog.Fee, _ = decimal.NewFromString(o.FillFees)
	return &OrderToLog
}

func FetchMarketIntervals() {
	const Hour int = 3600
	var Intervals []int = []int{Hour, 3 * Hour, 6 * Hour, 12 * Hour, 24 * Hour, 48 * Hour, 72 * Hour, 24 * 7 * Hour}
	LastRuns := make([]time.Time, len(Intervals))
	var Cycles int
	IntervalData := make(map[int]map[string]*okane.MarketRange)
	ShortestInterval := Intervals[0]
	log.Printf("Don't forget to put the intervals into a database record.\n")
	for true {
		Cycles++
		Sentry := Sentries.CheckEnsure("marketfetch", "pulling market stats, pass %d", Cycles)
		log.Printf("Attempting to sort out interval data...\n")
		Now := time.Now()
		var RangeUpdates int
		for k, Interval := range Intervals {
			Sentry.Checkin("Cycle %d, interval %d", Cycles, Interval)
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
			log.Printf("Stats for '%s' - '%s'\n", (time.Duration(Interval) * time.Second).String(), Start)
			res, err := rQ.PullInterval.Query(Start, okane.EXCHANGE_COINBASE)
			log.FatalIff(err, "Error pulling interval rates for '%d' with '%s'\n", Interval, Start)
			Results := make(map[string]*okane.MarketRange)
			ResultsUS := make(map[string]*okane.MarketRange)
			var Count int
			var DeadMarkets []string
			for res.Next() {
				err = res.Scan(&marketid, &max, &min, &maxusd, &minusd)
				log.FatalIff(err, "Scan error on FetchMarketIntervals()")
				MarketItem := okane.MarketLookup.ByIdOrDie(marketid)
				found, MarketDef := Global.Market.ByName(MarketItem.Name())
				if !found {
					log.Debugf("Skipping interval data for removed market '%s'\n", MarketItem.Name())
					DeadMarkets = append(DeadMarkets, MarketItem.Name())
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
			//log.Printf(" %d records retrieved.\n", Count)
			LastRuns[k] = Now
		}
		Bob := sjson.NewJsonFromObject(&IntervalData)
		log.ErrorIff(Bob.WriteToFile(Global.MarketIntervalsFile), "writing out json file '%s'", Global.MarketIntervalsFile)
		Sentry.Checkin("sleeping until pass %d", Cycles+1)
		time.Sleep(time.Minute)
	}
}

// TODO: Exponential backoff should be a thing.
func getMarketPriceLoop() {
	NewMarkets := okane.NewMarketDef()
	OldMarkets := okane.NewMarketDef()
	log.Printf("Async market-pricing thread started.\n")
	RateLimit := time.Second / 3
	var Count int
	Sentry := Sentries.NewSentry("getMarketPriceLoop()")
	for true {
		Count++
		Sentry.Checkin("remote market list pull, pass %d", Count)
		log.Debugf("Pulling product list...\n")
		Product, err := Global.readclient.GetProducts()
		if err != nil {
			log.Errorf("Failed to get products; waiting 1min to continue.\n")
			time.Sleep(time.Minute)
			continue
		}
		MarketRates := make(okane.MarketRatesType)
		UpdateTime := time.Now()
		BestBase := make(map[string]string)
		AllBases := make(map[string][]string)
		for _, v := range Product {
			AllBases[v.BaseCurrency] = append(AllBases[v.BaseCurrency], v.QuoteCurrency)
		}
		Scores := make(map[string]int)
		Scores["BTC"] = 0
		Scores["USD"] = 1
		Scores["USDC"] = 2
		Scores["DAI"] = 3
		Scores["USDT"] = 4
		Scores["EUR"] = 5
		Scores["GBP"] = 5
		for Coin := range AllBases {
			var Lowest int = 100
			for _, Base := range AllBases[Coin] {
				Score, found := Scores[Base]
				if found {
					if Score < Lowest {
						Lowest = Scores[Base]
						BestBase[Coin] = Base
					}
				}
			}
			if Lowest == 100 {
				log.Errorf("Coin '%s' had bases '%v'; no translation found.\n", Coin, AllBases[Coin])
			} else {
				//log.Printf("Coin '%s' found best adaptation '%s' (tier %d)\n", Coin, BestBase[Coin], Lowest)
			}
		}
		Global.LoadProgress = "no products loaded yet"
		for vi, v := range Product {
			Name := v.ID
			/*			if BestBase[v.BaseCurrency] != v.QuoteCurrency {
						log.Debugf("Currency '%s' best base is '%s'; we are ignoring market '%s'\n",
							v.BaseCurrency, BestBase[v.BaseCurrency], v.ID)
						continue
					}*/
			Market := &okane.MarketDef{
				Exchange:     okane.Exchanges.ByName["coinbase"],
				BaseCoin:     okane.CoinLookup.ByNameOrAdd(v.BaseCurrency),
				QuoteCoin:    okane.CoinLookup.ByNameOrAdd(v.QuoteCurrency),
				UsProhibited: false,
			}
			NewMarkets[Name] = Market
			if OldMarkets[Name] != nil {
				Market.LastRate = OldMarkets[Name].LastRate
			}
			Market.SetSymbol(Name)
			var Tries int
			var Ticker coinbasepro.Ticker
			var Done bool
			for Tries < 3 && !Done {
				Tries++
				log.Debugf("Pulling ticker data on '%s' (try %d)\n", v.ID, Tries)
				Ticker, err = Global.readclient.GetTicker(v.ID)
				if err != nil {
					if err.Error() == "Public rate limit exceeded" {
						log.Infof("Hit retry %d getting ticker '%s'\n", Tries, v.ID)
						time.Sleep(time.Second)
						continue
					} else if err.Error() == "Not allowed for delisted products" {
						log.Infof("Ticker '%s' delisted; can't pull\n", v.ID)
						Done = true
						continue
					} else {
						log.FatalIff(err, "Can't get ticker '%s' from coinbase.\n", v.ID)
					}
				} else {
					Done = true
				}
			}
			if Tries == 3 {
				log.Fatalf("Failed due to rate limit 3x fetching '%s'; exiting.\n", v.ID)
			}
			log.Debugf("Obtained ticker %s: %+v\n", v.ID, Ticker)
			if Market == nil {
				log.Debugf("No market obtained for '%s'; skipping.\n", v.ID)
				time.Sleep(RateLimit)
				continue
			}
			Last, _ := decimal.NewFromString(Ticker.Price)
			Bid, _ := decimal.NewFromString(Ticker.Bid)
			Ask, _ := decimal.NewFromString(Ticker.Ask)
			Quote := okane.MarketQuote{
				MarketDef:  Market,
				Last:       Last,
				Bid:        Bid,
				Ask:        Ask,
				LastUpdate: UpdateTime,
			}
			MarketRates[v.ID] = &Quote
			Market.LastRate = &Quote
			Global.LoadProgress = fmt.Sprintf("%s (%d/%d)",
				Market.Identifier(), vi, len(Product))
			Sentry.Checkin("remote market list pull, pass %d: %s", Count, Global.LoadProgress)
			time.Sleep(RateLimit)
		}
		MarketRates.SetFiat(nil)
		if NewMarkets.Equals(&OldMarkets) {
			log.Printf("Market data in '%s' has had no changes\n", Global.MarketDataFile)
		} else {
			Bob := sjson.NewJsonFromObject(&NewMarkets)
			log.ErrorIff(Bob.WriteToFile(Global.MarketDataFile), "writing out json file '%s'", Global.MarketDataFile)
			log.Printf("Writing market data to '%s' due to changes\n", Global.MarketDataFile)
		}
		Global.Market = &NewMarkets
		Global.MarketDataCycle++
		err = NewMarkets.UpdateOmarketsTable(okane.EXCHANGE_COINBASE)
		log.FatalIff(err, "Failed to update markets.\n")
		Bob := sjson.NewJsonFromObject(&MarketRates)
		log.ErrorIff(Bob.WriteToFile(Global.MarketRatesFile), "writing out json file '%s'", Global.MarketRatesFile)
		MarketRates.WriteMarketLast(Global.CentralRw, okane.Exchanges.ById[okane.EXCHANGE_COINBASE])
		Sentry.Checkin("waiting for wakeup for pass %d", Count+1)
		time.Sleep(Global.Timing.Poll)
	}
}

func LoadConfigValues(inifile string) {
	log.Init()
	log.SetThreshold(slogger.INFO)
	slogger.SetLogger(&log)
	shared.SetLogger(&log)
	okane.SetLogger(&log)
	SentryLog := slogger.Logger{}
	sentry.SetLogger(&SentryLog)
	SentryLog.SetThreshold(slogger.INFO)
	OtherIni := flag.String("inifile", "", "Specify an INI file for settings")
	ReadOnly := flag.Bool("readonly", false, "No writes to database or filesystem.")
	Test := flag.String("test", "", "Run specified test only.")
	Debug := flag.Bool("debug", false, "Enable verbose debugging.")
	flag.Parse()
	Cli.Inifile = *OtherIni
	Cli.ReadOnly = *ReadOnly
	Cli.Debug = *Debug
	Cli.Test = *Test
	Global.ReadOnly = Cli.ReadOnly
	if Cli.Debug {
		log.SetThreshold(slogger.DEBUG)
	}
	if Cli.Inifile != "" {
		inifile = Cli.Inifile
	}
	Config.SetDefaultIni(inifile).OrDie("Can't load ini")
	Config.PrintWarnings()
	Global.Pidfile = Config.GetStringOrDefault("pids.coinbase", "$HOME/coinbase.pid", "")
	Global.LocalStorage = Config.GetStringOrDie("coinbase.localstorage", "")
	Global.MarketDataFile = Config.GetStringOrDefault("coinbase.marketdata", Global.LocalStorage+"/marketdata-coinbase.json", "")
	Global.MarketRatesFile = Config.GetStringOrDefault("coinbase.marketrates", Global.LocalStorage+"/marketrates-coinbase.json", "")
	Global.MarketIntervalsFile = Config.GetStringOrDefault("coinbase.marketintervals", Global.LocalStorage+"/marketintervals-coinbase.json", "")
	_, Global.ChatSection = Config.GetString("coinbase.chathandle")

	found, List := Config.GetString("okane.userlist")
	if found {
		Global.UserNames = strings.Split(List, ",")
	}
}

func LoadCachedMarketData() error {
	Markets := Coinbase.GetMarketsDb(time.Hour * time.Duration(24))
	Global.Market = &Markets
	if len(Markets) > 100 {
		Global.MarketDataCycle = 1
	}
	return nil
}

func InitAndConnect() {
	Global.Timing.Poll = time.Minute
	Global.Timing.Timeout = time.Minute * 2
	Sentries = sentry.NewSentryTeam()
	Sentries.TripwireFunc = func(st *sentry.Sentry) {
		Global.ChatHandle.SendDefaultf("Coinbase-central rebooting due to failure on check %d of sentry %s (%s)\n",
			st.Counter(), st.Identifier(), st.Notes())
	}
	Sentries.DefaultTtl = Global.Timing.Timeout
	shared.ExitIfPidActive(Global.Pidfile)
	//Global.CentralRw = Config.ConnectDbKey("okane.centraldb").OrDie()
	Global.CentralRw = Config.ConnectDbKey("okane.centraldb").OrDie()
	okane.SetCentralDb(Global.CentralRw)
	log.FatalIff(okane.DbInit(Global.CentralRw), "dbinit failed.\n")
	Global.EventChannel = okane.NewEventChannel()
	if Global.ChatSection != "" {
		Global.ChatHandle = Config.NewChatHandle(Global.ChatSection).OrDie("failed to define chat handle")
	} else {
		Global.ChatHandle = &shared.ChatHandle{PrintChatOnly: true}
	}
	rQ.PullInterval = Global.CentralRw.PrepareOrDie("select marketid,max(last),min(last),max(lastusd),min(lastusd) from marketlast where timeat>$1 and exchangeid=$2 group by marketid;")
	// Top level section lists out the sections where the keys are defined.
	for _, Section := range Global.UserNames {
		Username := Config.GetStringOrDie(Section+".username", "Username is required")
		log.Printf("Init'ing user '%s'\n", Section)
		found, sList := Config.GetString(Section + ".coinbase")
		if !found {
			log.Printf("No coinbase handles for user '%s'\n", Section)
			continue
		} else {
			log.Printf("Defined coinbase handles '%s' for user '%s'\n", sList, Section)
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

		// Underneath the user are accounts, not users.
		for _, UserSection := range Lists {
			log.Printf("Scanning accounts for section '%s'; now doing '%s'\n", Section, UserSection)
			cbAccounts := User.LoadAccountsForExchange(Coinbase)
			for _, Account := range cbAccounts {
				log.Printf("Making Coinbase connection for %s\n", Account.Identifier())
				bAccount := ToCoinbaseAccount(Account, Section)
				bAccount.Exchange = Coinbase
				Global.Accounts = append(Global.Accounts, bAccount)
			}
		}
	}
	Global.EventChannel = okane.NewEventChannel()
	Global.EventChannel.Connect()
}

func (Acct *CoinbaseAccount) getCbBalance() []*okane.CoinBalance {
	url := "/coinbase-accounts"
	var Raw interface{}
	var iFace shared.Iface
	cbclient := Acct.Handle
	_, err := cbclient.Request("GET", url, nil, &Raw)
	/*if res != nil {
		log.Printf("Result: %v\n", res)
	} */
	if err != nil {
		log.Fatalf("Balance fetch fatal error '%s' fetching '%s'\n", err, url)
		os.Exit(2)
	}
	//log.Printf("Body return: %v\n", Raw)
	iFace.Absorb(Raw)
	//log.Printf("Json representation of results: %s\n", iFace.String())
	var Balances []*okane.CoinBalance
	for _, a := range iFace.SplitArray() {
		Balance := decimal.NewFromFloat(a.Key("balance").Float())
		Hold := decimal.NewFromFloat(a.Key("hold_balance").Float())
		if Balance.IsZero() && Hold.IsZero() {
			continue
		}
		Currency := a.Key("currency").String()
		Available := Balance.Sub(Hold)
		Rec := &okane.CoinBalance{
			Coin:      okane.CoinLookup.LabelToId(Currency, true),
			Balance:   Balance,
			Available: Available,
			Hold:      Hold,
		}
		Balances = append(Balances, Rec)
	}
	log.Printf("Coinbase balance check completed.\n")
	return Balances
}

func main() {
	LoadConfigValues("/data/config/coinbase-central.ini")
	InitAndConnect()
	defer Global.CentralRw.Close()
	Global.ChatHandle.SendErrorf("Coinbase central process bootup.\n")
	LoadCachedMarketData()

	if Cli.Test != "" {
		switch Cli.Test {
		case "chat":
			Global.ChatHandle.SendDefaultf("coinbase-central default output test\n")
			Global.ChatHandle.SendErrorf("coinbase-central diag/error output test\n")
			for _, v := range Global.Accounts {
				v.Notices.SendfIfDef("Testing chat output from %s\n",
					v.Identifier())
			}
		case "orders":
			log.Printf("Should check orders.\n")
			checkOrderLoop()
		case "coinbase":
			log.Printf("Trying to check Coinbase balances")
			updateBalanceLoop()
		}
		log.Printf("Test mode only; exiting.\n")
		os.Exit(2)
	}
	if Global.readclient == nil {
		log.Printf("Not one single coinbase client found; exiting.\n")
		os.Exit(1)
	}
	go getMarketPriceLoop()
	for Global.MarketDataCycle == 0 {
		log.Printf("Waiting for market prices to download; at %s\n", Global.LoadProgress)
		time.Sleep(time.Second)
	}
	go FetchMarketIntervals()
	go updateBalanceLoop()
	go checkOrderLoop()
	go placeOrderLoop()
	log.Printf("Completed startup, now monitoring threads.\n")
	for {
		//Sentries.ActiveStatus()
		time.Sleep(time.Second * 10)
	}
	//	time.Sleep(time.Hour - 5*time.Second)
	//	log.Printf("End of ~1hr process window.\n")
}
