package okane

import (
	"database/sql"
	"fmt"
	"github.com/grammaton76/g76golib/shared"
	"github.com/grammaton76/g76golib/slogger"
	"github.com/shopspring/decimal"
	"sync"
	"time"
)

var Config shared.Configuration

var global struct {
	CentralDb *shared.DbHandle
}

type EventReason string

const (
	EV_ORDER_OPEN   EventReason = "orderopen"
	EV_ORDER_UPDATE EventReason = "orderupdate"
	EV_ORDER_CLOSED EventReason = "orderclosed"
	EV_NEWRATES     EventReason = "newrates"
)

// Account is a downstream concept tied strictly to API keys
type Account struct {
	User     *User
	Exchange *Exchange
	CredId   int
	Cache    struct {
		Pending    map[string]*ActionRequest
		PendingIds map[int]*ActionRequest
		ResultSub  map[int]chan *ActionRequest
		xPending   sync.Mutex
		Open       map[string]*Order
		dOpen      map[string]chan *Order
		xOpen      sync.Mutex
		Closed     map[string]*Order
		xClosed    sync.Mutex
		seen       struct {
			Pending map[string]int
			Open    map[string]int
			Closed  map[string]int
		}
	}
	CacheCycle int
	X          map[string]interface{} `json:"-"`
}

// User represents an individual; a user may have many accounts, but one db only.
// Strategies may cross accounts if they're playing cross-exchange shenanigans like arb.
type User struct {
	Username     string
	Notices      *shared.ChatTarget     `json:"-"`
	Accounts     []*Account             `json:"-"`
	Db           *shared.DbHandle       `json:"-"`
	wQ           UserPreparedQueries_rw `json:"-"`
	rQ           UserPreparedQueries_ro `json:"-"`
	ClosedOrders map[string]bool        `json:"-"`
	Events       *EventChannel          `json:"-"`
}

type UserPreparedQueries_rw struct {
	BalanceWipeForAccount *shared.Stmt
	BalanceInsert         *shared.Stmt
	OrderOpenDelete       *shared.Stmt
	OrderOpenInsert       *shared.Stmt
	OrderOpenUpdateFill   *shared.Stmt
	OrderArchiveInsert    *shared.Stmt
	StratOrderUpdate      *shared.Stmt
	UpdateLogpoint        *shared.Stmt
	SetRequestStatusFrom  *shared.Stmt
	SetRequestMarkHandled *shared.Stmt
	SetRequestProcessed   *shared.Stmt
	RequestInsert         *shared.Stmt
	InsertStratOrder      *shared.Stmt
}

type UserPreparedQueries_ro struct {
	CredSelectByExchangeId *shared.Stmt
	OrderArchivePresent    *shared.Stmt
	CheckLogpoints         *shared.Stmt
	CheckTableMaxId        *shared.Stmt
	CredSelectAll          *shared.Stmt
	GetUserStrats          *shared.Stmt
	GetLastStratCloses     *shared.Stmt
	GetOpenRequests        *shared.Stmt
	GetRequest             *shared.Stmt
	GetOpenOrders          *shared.Stmt
	GetStratOrders         *shared.Stmt
	GetClosedOrder         *shared.Stmt
}

var wQ struct {
	writeMarketLast *shared.Stmt
	writeOmarkets   *shared.Stmt
}

var rQ struct {
	getMarketDefsForExchange  *shared.Stmt
	getMarketRatesForExchange *shared.Stmt
	getRateForMarket          *shared.Stmt
}

type ActionRequestSub struct {
	Id     chan int
	Result chan *ActionRequest
}

type ActionRequest struct {
	Account   *Account
	Id        int
	Command   string
	Status    string
	Uuid      string
	Market    *MarketDef
	Qty       float64
	Bidlimit  float64
	Stratid   int
	Label     string
	Requested time.Time
	Subscribe ActionRequestSub
	Error     error
}

var Exchanges struct {
	ById   map[int]*Exchange
	ByName map[string]*Exchange
}

type Exchange struct {
	Id              int
	Name            string
	Markets         MarketDefsType `json:"-"`
	LastMarketFetch time.Time
	cache           struct {
		LastMarketRates *MarketRatesType `json:"-"`
		LastMarketDefs  *MarketDefsType  `json:"-"`
	}
	LastRateFetch time.Time
	LoadCount     int
}

type FiatMap struct {
	BtcMarket  *MarketQuote
	EthMarket  *MarketQuote
	EuroMarket *MarketQuote
	GbpMarket  *MarketQuote
}

type MarketDef struct {
	shared.LookupItem `json:"MarketId"`
	Exchange          *Exchange
	BaseCoin          Coin
	QuoteCoin         Coin
	Status            string
	Created           time.Time
	UsProhibited      bool
	Notice            string
	Precision         decimal.Decimal
	LastRate          *MarketQuote `json:"-"`
}

type MarketQuote struct {
	MarketDef  *MarketDef
	Last       decimal.Decimal
	Bid        decimal.Decimal
	Ask        decimal.Decimal
	UsdRate    decimal.Decimal
	Volume     decimal.NullDecimal
	LastUpdate time.Time
}

type MarketRange struct {
	MarketDef  *MarketDef
	MinRate    decimal.Decimal
	MinRateUsd decimal.Decimal
	MaxRate    decimal.Decimal
	MaxRateUsd decimal.Decimal
}

type CoinBalance struct {
	Coin      Coin `json:"-"`
	Balance   decimal.Decimal
	Available decimal.Decimal
	Hold      decimal.Decimal
}

type Coin interface {
	shared.LookupItem
}

type OrderList struct {
	Orders         []*Order
	ByUuid         map[string]*Order
	ByDbid         map[int]*Order
	ActionSequence string
}

type Order struct {
	Account    *Account
	Uuid       string
	Label      string
	Type       string
	Market     *MarketDef `json:"-"`
	Base       Coin       `json:"-"`
	BaseCoin   string
	Quote      Coin `json:"-"`
	QuoteCoin  string
	Bidlimit   decimal.Decimal
	Quantity   decimal.Decimal
	Filled     decimal.Decimal
	Fee        decimal.Decimal
	UsdTotal   decimal.NullDecimal
	TotalPrice decimal.Decimal
	Created    time.Time
	Closed     time.Time
}

var OrderTranslationMapKraken = map[string]map[string]string{}

var OrderTranslationMapCbpro = map[string]map[string]string{
	"":      map[string]string{"sell": "LIMIT_SELL", "buy": "LIMIT_BUY"},
	"limit": map[string]string{"sell": "LIMIT_SELL", "buy": "LIMIT_BUY"},
}

var OrderTranslationMapBittrex = map[string]map[string]string{
	"LIMIT":  map[string]string{"SELL": "LIMIT_SELL", "BUY": "LIMIT_BUY"},
	"MARKET": map[string]string{"SELL": "MARKET_SELL", "BUY": "MARKET_BUY"},
}

const (
	EXCHANGE_BITTREX  int = 1
	EXCHANGE_BINANCE  int = 2
	EXCHANGE_SIM      int = 3
	EXCHANGE_COINBASE int = 4
	EXCHANGE_KRAKEN   int = 5
)

var log *slogger.Logger

type MarketRatesType map[string]*MarketQuote
type MarketDefsType map[string]*MarketDef
type IntervalSet map[string]*MarketRange

//var MarketDefs MarketDefsType
//var MarketRates MarketRatesType

var CoinLookup shared.LookupTable
var MarketLookup shared.LookupTable

func (list *OrderList) GetPL() {

}

func (exchange *Exchange) BuyFee() decimal.Decimal {
	return exchange.SellFee()
}

// We presently simplify EVERYTHING to 0.25% fee everywhere.
func (exchange *Exchange) SellFee() decimal.Decimal {
	return decimal.NewFromFloat(.0025)
}

func (md *MarketDef) Identifier() string {
	if md == nil {
		log.Errorf("market.Identifier() called on nil pointer!\n")
		return ""
	}
	Exchange := "UNDEFINED"
	if md.Exchange != nil {
		Exchange = md.Exchange.Name
	}
	return fmt.Sprintf("%s:%s", Exchange, md.Name())
}

func (md *MarketDef) SetSymbol(s string) {
	Def := MarketLookup.LabelToId(s, true)
	md.LookupItem = Def
}

func (md *MarketDef) LastUsd() decimal.Decimal {
	if md == nil {
		return decimal.Zero
	}
	if md.LastRate == nil {
		return decimal.Zero
	}
	return md.LastRate.UsdRate
}

func (md *MarketDef) Last() decimal.Decimal {
	if md == nil {
		return decimal.Zero
	}
	if md.LastRate == nil {
		return decimal.Zero
	}
	return md.LastRate.Last
}

func (mdt *MarketDefsType) List() []string {
	var List []string
	for _, v := range *mdt {
		List = append(List, v.Name())
	}
	return List
}

func NewMarketDef() MarketDefsType {
	md := make(MarketDefsType)
	return md
}

func (o *Order) Identifier() string {
	var Account string
	var Market string
	if o.Market != nil {
		Market = o.Market.Name()
	} else {
		Market = "mktNIL"
	}
	if o.Account != nil {
		if o.Account.User != nil {
			Account = fmt.Sprintf("%s@%s",
				o.Account.User.Username, o.Account.Exchange.Name)
		} else {
			Account = "userNIL"
		}
	} else {
		Account = "acctNIL"
	}
	var Unique string
	if o.Uuid != "" {
		Unique = o.Uuid
	} else {
		Unique = o.Created.String()
	}
	if o.Label != "" {
		Unique = fmt.Sprintf("%s(%s)", Unique, o.Label)
	}
	return fmt.Sprintf("%s:%s:%s\n", Account, Market, Unique)
}

func (md MarketRatesType) SetFiat(fm *FiatMap) {
	if fm == nil {
		fm = &FiatMap{
			BtcMarket:  md["BTC-USD"],
			EthMarket:  md["ETH-USD"],
			EuroMarket: md["USD-EUR"],
			GbpMarket:  md["BTC-GBP"],
		}
	}
	var EurUsd, BtcUsd, EthUsd, GbpUsd decimal.Decimal
	if fm.BtcMarket != nil {
		BtcUsd = fm.BtcMarket.Last
	}
	if fm.EthMarket != nil {
		EthUsd = fm.EthMarket.Last
	}
	if fm.EuroMarket != nil {
		EurUsd = fm.EuroMarket.Last
	}
	if fm.GbpMarket != nil {
		GbpUsd = fm.GbpMarket.Last
	}
	var Multiplier *decimal.Decimal
	for _, v := range md {
		Qc := v.MarketDef.QuoteCoin
		Bc := v.MarketDef.BaseCoin
		switch Qc.Name() {
		case "USD", "USDT", "USDC", "ZUSD":
			cMultiplier := decimal.NewFromInt(1)
			Multiplier = &cMultiplier
		case "EUR":
			Multiplier = &EurUsd
		case "GBP":
			Multiplier = &GbpUsd
		case "BTC", "XBT", "XXBT":
			Multiplier = &BtcUsd
		case "ETH":
			Multiplier = &EthUsd
		default:
			switch Bc.Name() {
			case "USD", "USDT", "USDC", "ZUSD":
				cMultiplier := decimal.NewFromInt(1)
				Multiplier = &cMultiplier
			case "EUR":
				Multiplier = &EurUsd
			case "BTC", "XBT", "XXBT":
				Multiplier = &BtcUsd
			case "ETH":
				Multiplier = &EthUsd
			default:
				log.Errorf("Base coin '%s' on market '%s' is unknown; value '%s'.\n",
					v.MarketDef.QuoteCoin.Name(), v.MarketDef.Name(), v.Last)
			}
		}
		if Multiplier != nil {
			v.UsdRate = v.Last.Mul(*Multiplier)
			v.MarketDef.LastRate = v
		}
	}
}

func (md *MarketRatesType) WriteMarketLast(dbh *shared.DbHandle, Exchange *Exchange) {
	BatchTime := time.Now()

	tx, err := okaneDb.Begin()
	if err != nil {
		log.Critf("ERROR on startup of transaction: %s!\n", err)
		return
	}
	for k, v := range *md {
		// exchangeid, marketid, timeat, coinid, basecoinid, last, ask, bid, lastusd
		_, err := tx.Stmt(wQ.writeMarketLast.Stmt).Exec(Exchange.Id, v.MarketDef.Id(), BatchTime,
			v.MarketDef.QuoteCoin.Id(), v.MarketDef.BaseCoin.Id(),
			v.Last, v.Ask, v.Bid, v.UsdRate)
		log.FatalIff(err, "Exchange %s marketrate insert error on record %d of market '%s'\n",
			Exchange.Identifier(), k, v.MarketDef.Name())
	}
	err = tx.Commit()
	log.ErrorIff(err, "failed transaction writing market rates for exchange %s",
		Exchange.Identifier())
}

func (md *MarketRatesType) PublishRates(acct *Account) error {
	return acct.Publish(EV_NEWRATES, md)
}

func (md *MarketDef) Equals(B *MarketDef) bool {
	if md == nil && B == nil {
		return true
	}
	if md == nil || B == nil {
		return false
	}
	if md.Name() != B.Name() || md.BaseCoin.Name() != B.BaseCoin.Name() || md.Status != B.Status || md.Created != B.Created || md.UsProhibited != B.UsProhibited {
		return false
	}
	return true
}

func SetLogger(l *slogger.Logger) *slogger.Logger {
	log = l
	return l
}

func (md MarketDefsType) ByName(item string) (bool, *MarketDef) {
	if md[item] == nil {
		return false, nil
	} else {
		return true, md[item]
	}
}

func (md MarketDefsType) ById(id int) (bool, *MarketDef) {
	MarketLookup := MarketLookup.ById(id)
	if MarketLookup == nil {
		return false, nil
	}
	Market := md[MarketLookup.Name()]
	if Market == nil {
		return false, nil
	}
	return true, md[Market.Name()]
}

func (md MarketDefsType) ByNameOrDie(item string) *MarketDef {
	if md[item] != nil {
		return md[item]
	}
	log.Fatalf("Failed to retrieve '%s' (len %d) from cached market data.\n", item, len(item))
	return nil
}

func (md *MarketDefsType) UpdateOmarketsTable(exchangeid int) error {
	for _, market := range *md {
		_, err := wQ.writeOmarkets.Exec(exchangeid, market.Id(), market.BaseCoin.Id(), market.QuoteCoin.Id(), market.QuoteCoin.Id(), market.Notice, market.Precision, market.UsProhibited)
		if err != nil {
			log.Errorf("Error updating market '%s': %s\n", market.Name(), err)
			return fmt.Errorf("db error writing market '%s': %s\n", market.Name(), err)
		}
	}
	return nil
}

func (md MarketDefsType) Equals(Old *MarketDefsType) bool {
	if md == nil && Old == nil {
		return true
	}
	if md == nil || Old == nil {
		log.Printf("Only one of New or Old was nil in equals.\n")
		return false
	}
	if len(md) != len(*Old) {
		log.Printf("Mismatch of market data sizes: %d vs %d\n", len(md), len(*Old))
		return false
	}
	for _, v := range md {
		if !md[v.Name()].Equals((*Old)[v.Name()]) {
			log.Printf("Change detected on market '%s'\n", v.Name)
			return false
		}
	}
	log.Debugf("No market metadata changes.\n")
	return true
}

var ExchangeBittrex = Exchange{
	Id:   EXCHANGE_BITTREX,
	Name: "bittrex",
}
var ExchangeBinance = Exchange{
	Id:   EXCHANGE_BINANCE,
	Name: "binance",
}
var ExchangeSimulator = Exchange{
	Id:   EXCHANGE_SIM,
	Name: "simulator",
}
var ExchangeCoinbase = Exchange{
	Id:   EXCHANGE_COINBASE,
	Name: "coinbase",
}
var ExchangeKraken = Exchange{
	Id:   EXCHANGE_KRAKEN,
	Name: "kraken",
}

func init() {
	Exchanges.ById = make(map[int]*Exchange)
	Exchanges.ByName = make(map[string]*Exchange)
	for _, v := range []*Exchange{&ExchangeBittrex, &ExchangeBinance, &ExchangeSimulator, &ExchangeCoinbase, &ExchangeKraken} {
		Exchanges.ById[v.Id] = v
		Exchanges.ByName[v.Name] = v
	}
}

var okaneDb *shared.DbHandle

func DbInit(dbh *shared.DbHandle) error {
	okaneDb = dbh
	CoinLookup = shared.NewLookup("coinlookup", dbh)
	MarketLookup = shared.NewLookup("marketlookup", dbh)
	wQ.writeMarketLast = dbh.PrepareOrDie(
		`INSERT INTO marketlast (exchangeid, marketid, timeat, coinid, basecoinid, last, ask, bid, lastusd) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`,
	)
	wQ.writeOmarkets = dbh.PrepareOrDie(
		`INSERT INTO omarkets (exchangeid, marketid, basecoinid, coinid, quotecurrency, notice, precision, us_restricted, firstseen, lastupdate) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'now', 'now') ON CONFLICT (exchangeid,marketid) DO UPDATE SET (notice,precision,lastupdate,us_restricted) = (EXCLUDED.notice, EXCLUDED.precision, EXCLUDED.lastupdate, EXCLUDED.us_restricted);`)
	rQ.getMarketDefsForExchange = dbh.PrepareOrDie("SELECT marketid, basecoinid, coinid, quotecurrency, notice, precision, us_restricted, firstseen, lastupdate FROM omarkets WHERE exchangeid=$1;")
	rQ.getMarketRatesForExchange = dbh.PrepareOrDie(
		"SELECT marketid,timeat,last,bid,ask,volume,lastusd from marketlast where timeat=(select max(timeat) from marketlast where exchangeid=$1) and exchangeid=$1;")
	rQ.getRateForMarket = dbh.PrepareOrDie("SELECT timeat,last,bid,ask,volume,lastusd from marketlast where exchangeid=$1 and marketid=$2 order by timeat desc limit 1;")
	return nil
}

func NewOrder(account *Account) *Order {
	var Order Order
	Order.Account = account
	return &Order
}

func (md *MarketDef) GetLastRateDb() *MarketQuote {
	sth, err := rQ.getRateForMarket.Query(md.Exchange.Id, md.Id())
	if err != nil {
		log.Fatalf("ERROR in execute for GetMarketRateDb '%s': %s\n", md.Identifier(), err)
	}
	defer sth.Close()
	Quote := MarketQuote{}
	for sth.Next() {
		var timeat time.Time
		err = sth.Scan(&timeat, &Quote.Last, &Quote.Bid, &Quote.Ask, &Quote.Volume, &Quote.UsdRate)
		log.FatalIff(err, "Failed to scan marketlast on GetMarketRatesDb()\n")
		Quote.MarketDef = md
		md.LastRate = &Quote
		//log.Debugf("Loaded market '%s' on exchange '%s'\n", Market.Name(), exchange.Name)
	}
	return &Quote
}

func (exchange *Exchange) GetMarketRatesDb() {
	sth, err := rQ.getMarketRatesForExchange.Query(exchange.Id)
	defer sth.Close()
	if err != nil {
		log.Fatalf("ERROR in execute for GetMarketRatesDb '%s': %s\n", exchange.Name, err)
	}
	for sth.Next() {
		var marketid int
		var timeat time.Time
		Quote := MarketQuote{}
		err = sth.Scan(&marketid, &timeat, &Quote.Last, &Quote.Bid, &Quote.Ask, &Quote.Volume, &Quote.UsdRate)
		log.FatalIff(err, "Failed to scan marketlast on GetMarketRatesDb()\n")
		found, Market := exchange.Markets.ById(marketid)
		if !found {
			log.Errorf("WEIRD: Marketid '%d' didn't resolve to a valid market on exchange '%s'\n", marketid, exchange.Name)
			continue
		}
		Quote.MarketDef = Market
		Market.LastRate = &Quote
		//log.Debugf("Loaded market '%s' on exchange '%s'\n", Market.Name(), exchange.Name)
	}
}

func (exchange *Exchange) Identifier() string {
	return fmt.Sprintf("%s", exchange.Name)
}

func (exchange *Exchange) GetMarketsDb(MaxAge time.Duration) MarketDefsType {
	log.Debugf("Pulling markets from db on exchange '%s'\n", exchange.Identifier())
	OldestAllowed := time.Now().Add(-MaxAge)
	if exchange.LastMarketFetch.Before(OldestAllowed) {
		log.Debugf("Last market fetch was at '%s', cutoff was '%s' - reloading.\n",
			exchange.LastMarketFetch, OldestAllowed)
	} else {
		log.Debugf("Last market fetch was at '%s', cutoff was '%s' - serving cached.\n",
			exchange.LastMarketFetch, OldestAllowed)
		return *exchange.cache.LastMarketDefs
	}
	sth, err := rQ.getMarketDefsForExchange.Query(exchange.Id)
	defer sth.Close()
	if err != nil {
		log.Fatalf("ERROR in execute for GetMarketsForExchange '%s': %s\n", exchange.Name, err)
	}
	Markets := MarketDefsType{}
	for sth.Next() {
		var marketid, basecoinid, coinid, quotecurrency int
		var nLastSeen, nCreated sql.NullTime
		//var LastSeen time.Time
		Market := MarketDef{
			Exchange: exchange,
		}
		err = sth.Scan(&marketid, &basecoinid, &coinid, &quotecurrency, &Market.Notice, &Market.Precision,
			&Market.UsProhibited, &nCreated, &nLastSeen)
		log.FatalIff(err, "Failed to scan omarkets for exchange '%s' in GetMarketsForExchange()\n", exchange.Identifier())
		if !nCreated.Valid {
			Market.Created = time.Time{}
		} else {
			Market.Created = nCreated.Time
		}
		Market.LookupItem = MarketLookup.ById(marketid)
		Market.BaseCoin = CoinLookup.ById(basecoinid)
		Market.QuoteCoin = CoinLookup.ById(quotecurrency)
		if exchange == nil {
			log.Fatalf("Nil exchange.\n")
		}
		log.Debugf("Loaded market '%s' on exchange '%s'\n", Market.Name(), exchange.Name)
		Markets[Market.Name()] = &Market
	}
	log.Debugf("Loop finished on exchange '%s'.\n", exchange.Name)
	exchange.Markets = Markets
	log.Debugf("Loaded %d markets on %s.\n", len(Markets), exchange.Identifier())
	exchange.cache.LastMarketDefs = &Markets
	exchange.LastMarketFetch = time.Now()
	exchange.LoadCount++
	return Markets
}

func (a *Account) SendOrderUpdate(o *Order) *EvOrderUpdate {
	Update := EvOrderUpdate{
		Market: o.Market,
		Uuid:   o.Uuid,
		Max:    o.Quantity,
		Filled: o.Filled,
		Active: true,
	}
	a.Publish(EV_ORDER_UPDATE, Update)
	return &Update
}

func (a *Account) AwaitRequestResult(Id int) chan *ActionRequest {
	a.Cache.xPending.Lock()
	a.Cache.ResultSub[Id] = make(chan *ActionRequest)
	a.Cache.xPending.Unlock()
	return a.Cache.ResultSub[Id]
}

func (a *Account) AwaitUuidNotOpen(Uuid string) chan *Order {
	a.Cache.xOpen.Lock()
	a.Cache.dOpen[Uuid] = make(chan *Order)
	a.Cache.xOpen.Unlock()
	return a.Cache.dOpen[Uuid]
}

func (o *Order) RecordOpenOrder() error {
	_, err := o.Account.User.wQ.OrderOpenInsert.Exec(
		o.Account.CredId, o.Uuid, o.Label, o.Type,
		o.Market.Id(), o.Market.Name(), o.Base.Id(), o.Base.Name(), o.Quote.Id(), o.Quote.Name(),
		o.Bidlimit, o.Quantity, o.Filled, o.Fee,
		o.Created)
	if err != nil {
		log.Fatalf("exec for o_order_open insert Error: %s\n", err)
	}
	if o.Account == nil {
		log.Infof("Recording opened order '%s' (account was nil)\n", o.Uuid)
	} else {
		log.Infof("Recording opened order '%s' for '%s'\n", o.Uuid, o.Account.Identifier())
	}
	o.Account.Publish(EV_ORDER_OPEN, o)
	return err
}

func (u *User) UuidCloseRecorded(Uuid string) bool {
	if u.ClosedOrders[Uuid] {
		return true
	}
	var Caw int
	res := u.rQ.OrderArchivePresent.QueryRow(Uuid)
	err := res.Scan(&Caw)
	if err == sql.ErrNoRows {
		return false
	}
	log.ErrorIff(err, "couldn't check o_order_archive for order '%s'", Uuid)
	u.ClosedOrders[Uuid] = true
	return true
}

func (o *Order) IsArchived() bool {
	if o.Account.User.ClosedOrders[o.Uuid] {
		return true
	}
	var Caw int
	res := o.Account.User.rQ.OrderArchivePresent.QueryRow(o.Uuid)
	err := res.Scan(&Caw)
	if err == sql.ErrNoRows {
		return false
	}
	log.ErrorIff(err, "couldn't check o_order_archive for order '%s'", o.Uuid)
	o.Account.User.ClosedOrders[o.Uuid] = true
	return true
}

func (u *User) RemoveOpenOrder(Uuid string) error {
	_, err := u.wQ.OrderOpenDelete.Exec(Uuid)
	if err != nil {
		log.Fatalf("exec for o_order_open delete Error: %s\n", err)
	}
	return err
}

func (o *Order) ExchangeName() string {
	if o.Market.Exchange != nil {
		return o.Market.Exchange.Name
	} else {
		if o.Account != nil {
			if o.Account.Exchange != nil {
				return o.Account.Exchange.Name
			}
		}
	}
	return "exchangeNIL"
}

func (o *Order) FormatOrderVanished() string {
	//	:heavy_dollar_sign: Executed limit-sell FTC-BTC 2500.00000000 @ 0.00000024 (0.00060000 / $18.13863548) on bittrex after 1d 5:41:38
	//	:truck: Executed limit-buy ADA-BTC 25.00000000 @ 0.00002033 (0.00050825 / $14.99320650) on bittrex after 0d 3:16:48
	var Prefix string
	var Body string
	switch o.Type {
	case "LIMIT_SELL":
		Prefix = "Sell order "
	case "LIMIT_BUY":
		Prefix = "Buy order "
	default:
		Prefix = fmt.Sprintf("Order of unknown type %s ", o.Type)
	}
	Body = fmt.Sprintf(" %s %s @ %s (%s) has vanished.",
		o.Market.Name(), o.Quantity.StringFixed(8), o.Bidlimit.StringFixed(8),
		o.ExchangeName())
	return Prefix + Body
}

func (o *Order) FormatOrderOpenChat() string {
	//	:heavy_dollar_sign: Executed limit-sell FTC-BTC 2500.00000000 @ 0.00000024 (0.00060000 / $18.13863548) on bittrex after 1d 5:41:38
	//	:truck: Executed limit-buy ADA-BTC 25.00000000 @ 0.00002033 (0.00050825 / $14.99320650) on bittrex after 0d 3:16:48
	var Prefix string
	var Body string
	switch o.Type {
	case "LIMIT_SELL":
		Prefix = "Placed limit-sell"
	case "LIMIT_BUY":
		Prefix = "Placed limit-buy"
	default:
		Prefix = fmt.Sprintf("Placed unknown type %s", o.Type)
	}
	Body = fmt.Sprintf(" %s %s @ %s on %s",
		o.Market.Name(), o.Quantity.StringFixed(8), o.Bidlimit.StringFixed(8),
		o.ExchangeName())
	return Prefix + Body
}

func (o *Order) FormatOrderClosedChat() string {
	//	:heavy_dollar_sign: Executed limit-sell FTC-BTC 2500.00000000 @ 0.00000024 (0.00060000 / $18.13863548) on bittrex after 1d 5:41:38
	//	:truck: Executed limit-buy ADA-BTC 25.00000000 @ 0.00002033 (0.00050825 / $14.99320650) on bittrex after 0d 3:16:48
	var Prefix string
	var Body string
	switch o.Type {
	case "LIMIT_SELL":
		Prefix = ":heavy_dollar_sign: Executed limit-sell"
	case "LIMIT_BUY":
		Prefix = ":truck: Executed limit-buy"
	default:
		Prefix = fmt.Sprintf(":question: Executed %s ", o.Type)
	}
	Body = fmt.Sprintf(" %s %s @ %s (%s) after %s",
		o.Market.Name(), o.Quantity.StringFixed(8), o.Bidlimit.StringFixed(8),
		o.ExchangeName(), o.Closed.Sub(o.Created).Truncate(time.Second).String())
	return Prefix + Body
}

func (o *Order) RecordClosedOrder() error {
	if o.IsArchived() {
		//log.Printf("Order '%s' was already present.\n", o.Uuid)
		return nil
	}
	//log.Printf("Recording closed order '%s'\n", o.Uuid)
	if o.Type == "" {
		log.Errorf("Closed order had blank type!\n")
		return fmt.Errorf("Closed order had blank type!\n")
	}
	_, err := o.Account.User.wQ.OrderArchiveInsert.Exec(
		o.Account.CredId, o.Account.Exchange.Id, o.Uuid, o.Type,
		o.Market.Id(), o.Market.Name(), o.Base.Id(), o.Base.Name(), o.Quote.Id(), o.Quote.Name(),
		o.Bidlimit, o.Quantity,
		o.Filled, o.Fee,
		o.Created, o.Closed, o.TotalPrice,
		o.Market.LastUsd())
	if err != nil {
		log.Fatalf("exec for o_order_open insert Error: %s\n", err)
	}
	o.Account.User.ClosedOrders[o.Uuid] = true
	o.Account.User.RemoveOpenOrder(o.Uuid)
	_, err = o.Account.User.wQ.StratOrderUpdate.Exec(o.Closed, o.Uuid)
	if err != nil {
		log.Fatalf("exec for stratorders update error: %s\n", err)
	}
	if o.Account == nil {
		log.Infof("Recording closed order '%s' (account was nil)\n", o.Uuid)
	} else {
		log.Infof("Recording closed order '%s' in '%s' for '%s'\n", o.Uuid, o.Market.Name(), o.Account.Identifier())
	}
	log.ErrorIff(o.Account.Publish(EV_ORDER_CLOSED, o), "failure recording closed order\n")
	return err
}

func NewUser(db *shared.DbHandle) *User {
	var user User
	user.DbInit(db)
	user.ClosedOrders = make(map[string]bool)
	return &user
}

func (a *Account) BalanceSnapshotInsert(Tx *sql.Tx, CoinId int, Balance decimal.Decimal, Available decimal.Decimal, Hold decimal.Decimal) (err error) {
	//log.Printf("Account %d coin lookup id '%d': %s\n", account.Id, CoinId, Balance)
	if Tx == nil {
		_, err = a.User.wQ.BalanceInsert.Exec(
			a.CredId, CoinId, Balance.String(), Available.String(), Hold.String())
	} else {
		_, err = Tx.Stmt(a.User.wQ.BalanceInsert.Stmt).Exec(
			a.CredId, CoinId, Balance.String(), Available.String(), Hold.String())
	}
	if err != nil {
		log.Printf("Can't insert balance snapshot row: %s\n", err)
	}
	return err
}

type BalState struct {
	CoinId    int
	Balance   decimal.Decimal
	Available decimal.Decimal
	Hold      decimal.Decimal
}

type BalanceSnapshot struct {
	account   *Account
	coinState map[int]*BalState
}

func (a *Account) NewBalanceSnapshot() *BalanceSnapshot {
	Ret := &BalanceSnapshot{
		account:   a,
		coinState: make(map[int]*BalState),
	}
	return Ret
}

func (bs *BalanceSnapshot) UpdateDb() error {
	log.Printf("Writing balances of %d coins to database.\n",
		len(bs.coinState))
	Tx, err := bs.account.User.Db.Begin()
	if err != nil {
		return log.ErrErrf("failed starting balance snapshot update transaction: %s", err)
	}
	bs.account.WipeBalanceSnapshot(Tx)
	for _, v := range bs.coinState {
		err = bs.account.BalanceSnapshotInsert(Tx, v.CoinId, v.Balance, v.Available, v.Hold)
	}
	return Tx.Commit()
}

func (bs *BalanceSnapshot) Add(Coinid int, Balance decimal.Decimal, Available decimal.Decimal, Hold decimal.Decimal) *BalanceSnapshot {
	Bal := bs.coinState[Coinid]
	if Bal == nil {
		Bal = &BalState{
			CoinId:    Coinid,
			Balance:   Balance,
			Available: Available,
			Hold:      Hold,
		}
	} else {
		Bal.Available.Add(Balance)
		Bal.Available.Add(Available)
		Bal.Hold.Add(Hold)
	}
	bs.coinState[Coinid] = Bal
	log.Debugf("Coin %d - added %s to balance.\n", Coinid, Balance.StringFixed(8))
	return bs
}

func (a *Account) OrderUpdatePartialFill(o *Order) error {
	_, err := a.User.wQ.OrderOpenUpdateFill.Exec(o.Filled.String(), o.Fee.String(), o.Uuid)
	log.ErrorIff(err, "Failed to update partial fill for UUID '%s'\n", o.Uuid)
	if o.Account == nil {
		log.Infof("Recording partial fill for order '%s' (account was nil)\n", o.Uuid)
	} else {
		log.Infof("%s: Recording partial fill for order '%s' (%s@%s, %s/%s)\n",
			o.Account.Identifier(), o.Uuid,
			o.Market.Name(), o.Bidlimit.StringFixed(8),
			o.Filled.StringFixed(8), o.Quantity.StringFixed(8))
	}
	log.ErrorIff(a.Publish(EV_ORDER_UPDATE, o), "error recording open order")
	return err
}

func (a *Account) WipeBalanceSnapshot(Tx *sql.Tx) (err error) {
	log.Debugf("Wiping balances for account '%s' (%d)\n", a.Identifier(), a.CredId)
	if Tx == nil {
		_, err = a.User.wQ.BalanceWipeForAccount.Exec(a.CredId)
	} else {
		_, err = Tx.Stmt(a.User.wQ.BalanceWipeForAccount.Stmt).Exec(a.CredId)
	}
	return err
}

func (u *User) DbInit(Db *shared.DbHandle) {
	if Db == nil {
		log.Fatalf("DB handle passed to okane.user.DbInit was null.\n")
	}
	u.Db = Db
	u.rQ.CredSelectByExchangeId = Db.PrepareOrDie(
		`SELECT id,credlabel FROM exchange_creds WHERE exchangeid=$1 AND active=true and suspended=false;`)
	u.rQ.CredSelectAll = Db.PrepareOrDie(
		`SELECT id,credlabel,exchangeid FROM exchange_creds WHERE active=true and suspended=false;`)
	//	u.OrderOpenForCred = shared.PrepareOrDie(u.Db,
	//		"SELECT id,uuid,filled FROM o_order_open WHERE credid=$1;")
	u.rQ.GetUserStrats = shared.PrepareOrDie(Db,
		`SELECT id, exchangeid, name, market, active, suspended, stratdata FROM stratdata;`)
	u.rQ.GetLastStratCloses = shared.PrepareOrDie(Db,
		"select stratowner,max(closed) from stratorders where closed is not null group by stratowner;")
	u.wQ.InsertStratOrder = shared.PrepareOrDie(Db,
		`INSERT INTO stratorders (credid, uuid, stratowner, market, type, quantity, price, active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`)
	u.rQ.GetOpenRequests = shared.PrepareOrDie(Db,
		`SELECT id,command,uuid,marketid,market,quantity,bidlimit,stratid,label,requested,status FROM o_order_request WHERE credid=$1 AND processed IS NULL AND status = 'REQUESTED';`)
	u.rQ.GetRequest = shared.PrepareOrDie(Db,
		`SELECT id,command,uuid,marketid,market,quantity,bidlimit,stratid,label,requested,status FROM o_order_request WHERE credid=$1 AND id=$2;`)
	u.rQ.OrderArchivePresent = shared.PrepareOrDie(Db,
		`SELECT 1 FROM o_order_archive WHERE uuid=$1;`)
	u.rQ.CheckLogpoints = shared.PrepareOrDie(Db,
		"SELECT lastid,logtype FROM slack_logpoint;")
	u.rQ.GetStratOrders = shared.PrepareOrDie(Db,
		"select credid,uuid,market,type,closed,filled,bidlimit from o_order_archive where uuid in (select uuid from stratorders where cancelled<>true and stratowner=$1) order by opened;")
	u.wQ.BalanceWipeForAccount = shared.PrepareOrDie(Db,
		`DELETE FROM okane_balances WHERE credid=$1;`)
	u.rQ.GetOpenOrders = shared.PrepareOrDie(Db,
		`SELECT uuid, label, type, marketid, bidlimit, quantity, filled, fee, opened, id FROM o_order_open WHERE credid=$1;`)
	u.wQ.BalanceInsert = shared.PrepareOrDie(Db,
		`INSERT INTO okane_balances (credid, coinid, balance, available, pending) VALUES ($1, $2, $3, $4, $5);`)
	u.wQ.OrderOpenInsert = shared.PrepareOrDie(Db,
		"INSERT INTO o_order_open (credid, uuid, label, type, marketid, market, baseid, base, coinid, coin, bidlimit, quantity, filled, fee, opened) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);")
	u.wQ.OrderOpenUpdateFill = shared.PrepareOrDie(Db,
		"UPDATE o_order_open SET filled=$1, fee=$2 WHERE uuid=$3;")
	u.wQ.OrderOpenDelete = shared.PrepareOrDie(Db,
		"DELETE FROM o_order_open WHERE uuid=$1;")
	u.wQ.OrderArchiveInsert = shared.PrepareOrDie(Db,
		`INSERT INTO o_order_archive (credid, exchangeid, uuid, type, marketid, market, baseid, base, coinid, coin, bidlimit, quantity, filled, fee, opened, closed, totalprice, usdrate) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);`)
	u.wQ.StratOrderUpdate = shared.PrepareOrDie(Db,
		`UPDATE stratorders SET closed=$1 WHERE uuid=$2;`)
	u.wQ.UpdateLogpoint = shared.PrepareOrDie(Db,
		"UPDATE slack_logpoint SET lastid=$1 WHERE logtype=$2;")
	u.wQ.SetRequestStatusFrom = shared.PrepareOrDie(Db,
		`UPDATE o_order_request SET status=$3 WHERE id=$1 AND status=$2;`)
	u.wQ.SetRequestProcessed = shared.PrepareOrDie(Db,
		`UPDATE o_order_request SET uuid=$2, processed=NOW(), status=$3 WHERE id=$1;`)
	u.wQ.RequestInsert = shared.PrepareOrDie(Db,
		"INSERT INTO o_order_request (status,stratid,credid,command,marketid,market,quantity,bidlimit,label) VALUES ('REQUESTED', $1, $2, $3, $4, $5, $6, $7, $8) RETURNING id;")
	u.rQ.GetClosedOrder = shared.PrepareOrDie(Db,
		"select uuid,marketid,type,closed,filled,bidlimit,quantity,opened from o_order_archive where uuid=$1;")
}

func (o *ActionRequest) MarkAsHandled(status string) error {
	switch status {
	case "REJECTED", "PLACED":
	default:
		return fmt.Errorf("Action request %s - cannot mark non-final status '%s' as processed.\n",
			o.Identifier(), status)
	}
	res, err := o.Account.User.wQ.SetRequestProcessed.Exec(o.Id, o.Uuid, status)
	if err != nil {
		return err
	}
	var Rows int64
	Rows, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if Rows == 1 {
		return nil
	}
	if o.Stratid != 0 {
		if status == "PLACED" {
			o.Account.RecordStratOrder(o)
		}
	}
	return fmt.Errorf("request %d somehow affected %d rows at MarkAsHandled()",
		o.Id, Rows)
}

func (o *ActionRequest) Identifier() string {
	if o.Label != "" {
		return fmt.Sprintf("order-req %s (%d) - %s, %.08f*%.08f",
			o.Label, o.Id, o.Market.Name(), o.Qty, o.Bidlimit)
	}
	return fmt.Sprintf("order-req %d - (%s, %.08f*%.08f)",
		o.Id, o.Market.Name(), o.Qty, o.Bidlimit)
}

func (a *Account) GetClosedOrder(uuid string) (*Order, error) {
	if a.Cache.Closed[uuid] != nil {
		return a.Cache.Closed[uuid], nil
	}
	res := a.User.rQ.GetClosedOrder.QueryRow(uuid)
	var s = &Order{
		Account: a,
	}
	var marketid sql.NullInt32
	var qty sql.NullFloat64
	var filled sql.NullFloat64
	var bidlimit sql.NullFloat64
	var opened sql.NullString
	var found bool
	err := res.Scan(&s.Uuid, &marketid, &s.Type, &s.Closed, &filled, &bidlimit, &qty, &opened)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		log.Fatalf("Account %s failed to query order archive for uuid '%s': %s\n",
			a.Identifier(), uuid, err)
		return nil, err
	}
	if qty.Valid {
		s.Quantity = decimal.NewFromFloat(qty.Float64)
	}
	if bidlimit.Valid {
		s.Bidlimit = decimal.NewFromFloat(bidlimit.Float64)
	}
	if marketid.Valid {
		imarketid := int(marketid.Int32)
		found, s.Market = a.Exchange.Markets.ById(imarketid)
		if !found {
			err = fmt.Errorf("Account %s order: Couldn't resolve market id %d on o_order_archive uuid %s\n",
				a.Identifier(), imarketid, s.Uuid)
			return nil, err
		}
	} else {
		err = fmt.Errorf("Account %s order: o_order_archive has unparseable marketid on uuid %s\n",
			a.Identifier(), s.Uuid)
		return nil, err
	}
	a.Cache.xClosed.Lock()
	a.Cache.Closed[s.Uuid] = s
	a.Cache.xClosed.Unlock()
	return s, nil
}

func (a *Account) RecordStratOrder(r *ActionRequest) error {
	log.Printf("stratorder for %s: stratid %d, inserting uuid %s if id is non-zero.\n",
		a.Identifier(), r.Stratid, r.Uuid)
	if r.Stratid == 0 {
		return nil
	}
	_, err := a.User.wQ.InsertStratOrder.Exec(r.Account.CredId, r.Uuid, r.Stratid, r.Market.Name(),
		r.Command, r.Qty, r.Bidlimit, true)
	if err != nil {
		log.Errorf("Failed to record strategy order on uuid '%s' when it closed.\n",
			r.Uuid)
	}
	return err
}

func (a *Account) SetRequest(o *ActionRequest, From string, To string) (bool, error) {
	if To == "PLACED" {
		log.Errorf("It is likely you should have called MarkAsHandled() (thus recording uuid) instead of SetRequest on order %s.\n", o.Identifier())
	}
	res, err := a.User.wQ.SetRequestStatusFrom.Exec(o.Id, From, To)
	if err != nil {
		return false, err
	}
	var Rows int64
	Rows, err = res.RowsAffected()
	if err != nil {
		return false, err
	}
	if Rows == 1 {
		return true, nil
	}
	return false, nil
}

func (a *Account) SendRequest(o *ActionRequest) error {
	row := a.User.wQ.RequestInsert.QueryRow(o.Stratid, a.CredId, o.Command, o.Market.Id(),
		o.Market.Name(), o.Qty, o.Bidlimit, o.Label)
	var Id int
	err := row.Scan(&Id)
	if err != nil {
		log.Debugf("failed to add o_order_request: %s", o.Identifier(), err)
		return err
	}
	o.Id = Id
	if o.Subscribe.Id != nil {
		o.Subscribe.Id <- int(Id)
	}
	if o.Subscribe.Result != nil {
		a.Cache.ResultSub[int(Id)] = o.Subscribe.Result
		log.Printf("Subscribed channel into cache watch list for id %d.\n",
			o.Id)
	}
	return err
}

func (a *Account) RunOrderCache(duration time.Duration, Cycle *int) {
	if a.Cache.Open != nil {
		log.Fatalf("Account %s already has a Cache process started. Re-think your life choices.\n",
			a.Identifier())
	}
	log.Printf("Starting cache process for account %s\n", a.Identifier())
	a.Cache.seen.Open = make(map[string]int)
	a.Cache.seen.Closed = make(map[string]int)
	a.Cache.seen.Pending = make(map[string]int)
	a.Cache.Open = make(map[string]*Order)
	a.Cache.Closed = make(map[string]*Order)
	a.Cache.Pending = make(map[string]*ActionRequest)
	a.Cache.ResultSub = make(map[int]chan *ActionRequest)
	a.Cache.PendingIds = make(map[int]*ActionRequest)
	a.Cache.dOpen = make(map[string]chan *Order)
	for true {
		var Pending int
		var Open int
		log.Printf("Account %s Cache-refresh cycle %d\n", a.Identifier(), a.CacheCycle)
		pending, err := a.GetOpenOrderRequests()
		if err != nil {
			log.Errorf("Account %s failed GetOpenOrderRequests: %s\n",
				a.Identifier(), err)
		}
		Pending = len(pending)
		for _, v := range pending {
			if a.Cache.Pending[v.Label] == nil {
				a.Cache.xPending.Lock()
				a.Cache.Pending[v.Label] = v
				a.Cache.PendingIds[v.Id] = v
				a.Cache.xPending.Unlock()
			} else {
				if v.Uuid != a.Cache.Pending[v.Label].Uuid {
					a.Cache.Pending[v.Label].Uuid = v.Uuid
				}
			}
			a.Cache.seen.Pending[v.Label] = a.CacheCycle
		}
		orders, err := a.GetOpenOrders()
		if err != nil {
			log.Fatalf("Account %s failed GetOpenOrders: %s\n",
				a.Identifier(), err)
		}
		Open = len(orders.Orders)
		for _, v := range orders.Orders {
			if a.Cache.Open[v.Uuid] == nil {
				a.Cache.xOpen.Lock()
				a.Cache.Open[v.Uuid] = v
				a.Cache.xOpen.Unlock()
			}
			a.Cache.seen.Open[v.Uuid] = a.CacheCycle
		}
		for k := range a.Cache.seen.Pending {
			if a.Cache.seen.Pending[k] != a.CacheCycle {
				log.Printf("Cache: %s is no longer in Pending.\n", k)
				Id := a.Cache.Pending[k].Id
				a.Cache.xPending.Lock()
				if a.Cache.ResultSub[Id] != nil {
					log.Printf("Subscription channel was open on pending request %d; pulling final disposition\n", Id)
					res, err := a.GetRequest(Id)
					log.FatalIff(err, "Failed to load o_order_request %d from db: %s\n", Id, err)
					a.Cache.ResultSub[Id] <- res
					close(a.Cache.ResultSub[Id])
					delete(a.Cache.ResultSub, Id)
				} else {
					log.Printf("Nothing subscribed to request %d\n", Id)
				}
				delete(a.Cache.Pending, k)
				delete(a.Cache.seen.Pending, k)
				a.Cache.xPending.Unlock()
			}
		}
		for k := range a.Cache.seen.Open {
			if a.Cache.seen.Open[k] != a.CacheCycle {
				a.Cache.xOpen.Lock()
				if a.Cache.dOpen[k] != nil {
					a.Cache.dOpen[k] <- a.Cache.Open[k]
					close(a.Cache.dOpen[k])
					delete(a.Cache.dOpen, k)
				}
				delete(a.Cache.Open, k)
				delete(a.Cache.seen.Open, k)
				a.Cache.xOpen.Unlock()
			}
		}
		log.Printf("Account %s Cache-refresh cycle %d: %d/%d pending watched, %d/%d open watched\n",
			a.Identifier(), a.CacheCycle, len(a.Cache.ResultSub), Pending, len(a.Cache.dOpen), Open)
		a.CacheCycle++
		if Cycle != nil {
			*Cycle = a.CacheCycle
		}
		time.Sleep(duration)
	}
}

func (a *Account) GetOpenOrders() (List *OrderList, err error) {
	res, err := a.User.rQ.GetOpenOrders.Query(a.CredId)
	if err != nil {
		log.Errorf("Account %s db error pulling o_order_request: %s\n",
			a.Identifier(), err)
		return nil, err
	}
	List = &OrderList{}
	List.ByUuid = make(map[string]*Order)
	List.ByDbid = make(map[int]*Order)
	for res.Next() {
		var s = &Order{
			Account: a,
		}
		var marketid sql.NullInt32
		var qty sql.NullFloat64
		var filled sql.NullFloat64
		var fee sql.NullFloat64
		var bidlimit sql.NullFloat64
		var label sql.NullString
		var opened sql.NullString
		var found bool
		var dbid int
		err = res.Scan(&s.Uuid, &label, &s.Type, &marketid, &bidlimit, &qty, &filled, &fee, &opened, &dbid)
		if err != nil {
			log.Fatalf("Account %s failed to query order requests: %s\n",
				a.Identifier(), err)
			return nil, err
		}
		if qty.Valid {
			s.Quantity = decimal.NewFromFloat(qty.Float64)
		}
		if bidlimit.Valid {
			s.Bidlimit = decimal.NewFromFloat(bidlimit.Float64)
		}
		if marketid.Valid {
			imarketid := int(marketid.Int32)
			found, s.Market = a.Exchange.Markets.ById(imarketid)
			if !found {
				err = fmt.Errorf("Account %s order: Couldn't resolve market id %d on o_order_open uuid %s\n",
					a.Identifier(), imarketid, s.Uuid)
				return nil, err
			}
		} else {
			err = fmt.Errorf("Account %s order: o_order_open has unparseable marketid on uuid %s\n",
				a.Identifier(), s.Uuid)
			return nil, err
		}
		List.ByUuid[s.Uuid] = s
		List.ByDbid[dbid] = s
		List.Orders = append(List.Orders, s)
	}
	return List, nil
}

func (a *Account) GetOpenOrderRequests() (List []*ActionRequest, errors error) {
	res, err := a.User.rQ.GetOpenRequests.Query(a.CredId)
	return a.getRequests(res, err)
}

func (a *Account) GetRequest(Id int) (*ActionRequest, error) {
	res, errorig := a.User.rQ.GetRequest.Query(a.CredId, Id)
	List, err := a.getRequests(res, errorig)
	if err != nil {
		return nil, err
	}
	if len(List) == 0 {
		return nil, nil
	}
	return List[0], nil
}

func (a *Account) getRequests(res *sql.Rows, err error) (List []*ActionRequest, errors error) {
	if err != nil {
		log.Errorf("Account %s db error pulling o_order_request: %s\n", a.Identifier(), err)
		return nil, err
	}
	for res.Next() {
		var s = &ActionRequest{
			Account: a,
		}
		var marketid sql.NullInt32
		var marketname sql.NullString
		var qty sql.NullFloat64
		var bidlimit sql.NullFloat64
		var stratid sql.NullInt64
		var uuid sql.NullString
		var found bool
		err = res.Scan(&s.Id, &s.Command, &uuid, &marketid, &marketname, &qty, &bidlimit, &stratid, &s.Label, &s.Requested, &s.Status)
		if err != nil {
			log.Errorf("Account %s failed to query order requests: %s\n",
				a.Identifier(), err)
			return nil, err
		}
		if uuid.Valid {
			s.Uuid = uuid.String
		}
		if stratid.Valid {
			s.Stratid = int(stratid.Int64)
		}
		if qty.Valid {
			s.Qty = qty.Float64
		}
		if bidlimit.Valid {
			s.Bidlimit = bidlimit.Float64
		}
		if marketid.Valid {
			found, s.Market = a.Exchange.Markets.ById(int(marketid.Int32))
			if !found {
				err = fmt.Errorf("Account %s order request: Couldn't resolve market id %d on order_request %d\n",
					a.Identifier(), marketid, s.Id)
				return nil, err
			}
		} else {
			found, s.Market = a.Exchange.Markets.ByName(marketname.String)
			if !found {
				err = fmt.Errorf("Account %s order request: Couldn't resolve market name '%s' on order_request %d\n",
					a.Identifier(), marketname, s.Id)
				return nil, err
			}
		}
		List = append(List, s)
	}
	return List, nil
}

func (u *User) Identifier() string {
	return fmt.Sprintf("%s", u.Db.DbName)
}

func SetCentralDb(Db *shared.DbHandle) {
	if Db == nil {
		log.Fatalf("DB handle passed to okane.DbInit was null.\n")
	}
	global.CentralDb = Db
}

func (a *Account) Identifier() string {
	if a == nil {
		return "nil_Account"
	}
	return fmt.Sprintf("%s:%d", a.User.Identifier(), a.CredId)
}

func (u *User) LoadAccountsForExchange(exchange *Exchange) []*Account {
	var Accounts []*Account
	CredSelect, err := u.rQ.CredSelectByExchangeId.Query(exchange.Id)
	defer CredSelect.Close()
	if err != nil {
		log.Fatalf("Execute for LoadAccountsForExchange '%s': %s\n", exchange.Identifier(), err)
	}
	for CredSelect.Next() {
		var (
			credid int
			label  *string
		)
		var Account Account
		err = CredSelect.Scan(&credid, &label)
		if err != nil {
			panic(err.Error())
		}
		Account.CredId = credid
		Account.User = u
		Account.Exchange = exchange
		if Account.Exchange.LoadCount == 0 {
			Account.Exchange.GetMarketsDb(time.Hour)
		}
		Account.X = make(map[string]interface{})
		//log.Printf("Found credential id %d\n", Account.Identifier())
		Accounts = append(Accounts, &Account)
	}
	return Accounts
}

func (u *User) LoadAllAccounts() []*Account {
	var Accounts []*Account
	CredSelect, err := u.rQ.CredSelectAll.Query()
	defer CredSelect.Close()
	if err != nil {
		log.Fatalf("ERROR in execute for LoadAllAccounts(): %s\n", err)
	}
	for CredSelect.Next() {
		var (
			credid     int
			exchangeid int
			label      *string
		)
		var Account Account
		err = CredSelect.Scan(&credid, &label, &exchangeid)
		if err != nil {
			panic(err.Error())
		}
		Account.CredId = credid
		Account.User = u
		Account.Exchange = Exchanges.ById[exchangeid]
		if Account.Exchange.LoadCount == 0 {
			Account.Exchange.GetMarketsDb(time.Hour)
		}
		Account.X = make(map[string]interface{})
		//log.Printf("Found credential id %d\n", Account.Identifier())
		Accounts = append(Accounts, &Account)
	}
	return Accounts
}
