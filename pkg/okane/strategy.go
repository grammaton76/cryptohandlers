package okane

import (
	"database/sql"
	"fmt"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/shopspring/decimal"
	"time"
)

type Strategy struct {
	Id          int
	Exchange    *Exchange
	Name        string
	User        *User
	Market      *MarketDef
	Active      bool
	Suspended   bool
	Data        sjson.JSON
	SkipBecause error
}

type StratLastUpdate map[int]time.Time

type StratList []*Strategy

func (List StratList) ToMap() map[int]*Strategy {
	Map := make(map[int]*Strategy)
	for _, v := range List {
		Map[v.Id] = v
	}
	return Map
}

func (Strat *Strategy) Identifier() string {
	if Strat == nil {
		return "(nil Strategy)"
	}
	return fmt.Sprintf("%d", Strat.Id)
}

func (u *User) LastStratCloses() map[int]time.Time {
	LastOrders := make(map[int]time.Time)
	Closes, err := u.rQ.GetLastStratCloses.Query()
	if err != nil {
		log.Fatalf("getting latest close for '%s': %s\n",
			u.Identifier(), err)
	}
	for Closes.Next() {
		var StratId int
		var Closed *time.Time
		err = Closes.Scan(&StratId, &Closed)
		if err != nil {
			panic(err.Error())
		}
		log.Debugf("Latest close for strat %d was %s\n", StratId, Closed)
		if Closed == nil || Closed.IsZero() {
			log.Printf("Null time for most recent close on strategy %d\n", StratId)
			continue
		}
		if LastOrders[StratId].Equal(*Closed) {
			log.Debugf("No newly closed orders detected for strategy %d\n", StratId)
			continue
		}
		LastOrders[StratId] = *Closed
	}
	return LastOrders
}

func (strat *Strategy) GetOrderList() (*OrderList, error) {
	var err error
	var ClosedActions string
	var Return OrderList
	Orders, err := strat.User.rQ.GetStratOrders.Query(strat.Id)
	if err != nil {
		log.Fatalf("ERROR getting latest close for '%s': %s\n", strat.Identifier(), err)
	}
	for Orders.Next() {
		var r struct {
			Exchangeid               int
			Uuid, oMarketName, oType string
			oClosed                  *time.Time
			oQty, oPrice             decimal.Decimal
		}
		err = Orders.Scan(&r.Exchangeid, &r.Uuid, &r.oMarketName, &r.oType, &r.oClosed, &r.oQty, &r.oPrice)
		if err != nil {
			panic(err.Error())
		}
		Exchange := Exchanges.ById[r.Exchangeid]
		Exchange.GetMarketsDb(time.Hour)
		Market := MarketLookup.ByName(r.oMarketName)
		var ThisDirection string
		switch r.oType {
		case "LIMIT_BUY":
			ThisDirection = "B"
		case "LIMIT_SELL":
			ThisDirection = "S"
		default:
			log.Fatalf("Unknown order type '%s' encountered on market %s\n", r.oType, Market)
		}
		ClosedActions += ThisDirection
		if len(ClosedActions) > 1 {
			if ClosedActions[:2] == "BS" || ClosedActions[:2] == "SB" {
				//log.Printf("Reversal: %s ends with %s\n", ClosedActions, ClosedActions[:2])
			}
		}
		//  id | credid | uuid | type |  market  | base | coin | bidlimit | quantity | filled | fee |  opened | closed | raw | totalprice | marketid | baseid | coinid |    usdrate
		Found, Marketdef := Exchange.Markets.ByName(r.oMarketName)
		if !Found {
			log.Printf("Market '%s' not found on %s; skipping.\n", r.oMarketName, Exchange.Name)
			continue
		}
		O := &Order{
			Account:    nil,
			Uuid:       r.Uuid,
			Type:       r.oType,
			Market:     Marketdef,
			Bidlimit:   decimal.Decimal{},
			Quantity:   r.oQty,
			Filled:     decimal.Decimal{},
			Fee:        decimal.Decimal{},
			UsdTotal:   decimal.NullDecimal{},
			TotalPrice: r.oPrice,
			Created:    time.Time{},
			Closed:     time.Time{},
		}
		O.Base = O.Market.BaseCoin
		O.Quote = O.Market.QuoteCoin
		O.BaseCoin = O.Base.Name()
		O.QuoteCoin = O.Quote.Name()
		Return.Orders = append(Return.Orders, O)
	}
	//log.Printf("History %s\n", ClosedActions)
	Orders.Close()
	Return.ActionSequence = ClosedActions
	return &Return, nil
}

func (u *User) GetStrats() (StratList, error) {
	var Ret []*Strategy
	res, err := u.rQ.GetUserStrats.Query()
	if err != nil {
		panic(err.Error())
	}
	defer res.Close()
	for res.Next() {
		var r struct {
			id, exchangeid    sql.NullInt64
			name, market      string
			active, suspended bool
			stratdata         sql.NullString
		}
		err = res.Scan(&r.id, &r.exchangeid, &r.name, &r.market, &r.active, &r.suspended, &r.stratdata)
		if err != nil {
			panic(err.Error())
		}
		V := &Strategy{
			Id:        int(r.id.Int64),
			Name:      r.name,
			User:      u,
			Active:    r.active,
			Suspended: r.suspended,
			Data:      nil,
		}
		Ret = append(Ret, V)
		if !r.exchangeid.Valid {
			V.SkipBecause = fmt.Errorf("null exchangeid")
			V.Suspended = true
		}
		vEx := Exchanges.ById[int(r.exchangeid.Int64)]
		V.Exchange = vEx
		vEx.GetMarketsDb(time.Hour)
		found, vMarket := vEx.Markets.ByName(r.market)
		if !found {
			log.Printf("Apparently %s no longer supports market '%s'; skipping strategy.\n",
				vEx.Name, r.market)
			continue
		}
		V.Market = vMarket
	}
	return Ret, nil
}
