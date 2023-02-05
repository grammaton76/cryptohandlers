package okane

import (
	"fmt"
	"github.com/shopspring/decimal"
)

type Stairway struct {
	Delta    decimal.Decimal
	Ratio    decimal.Decimal
	Minimum  decimal.Decimal
	stepsF64 []float64
	stepsDec []decimal.Decimal
}

func (Sta *Stairway) String() string {
	return fmt.Sprintf("%s", Sta.Delta.String())
}

func NewStairwayF(Delta float64, Minimum float64) *Stairway {
	return NewStairway(decimal.NewFromFloat(Delta), decimal.NewFromFloat(Minimum))
}

var cMaxStairstep decimal.Decimal
var cMinimum decimal.Decimal

func init() {
	cMaxStairstep = decimal.NewFromInt(50000)
	cMinimum, _ = decimal.NewFromString("0.00000001")
}

func NewStairway(Ratio decimal.Decimal, Minimum decimal.Decimal) *Stairway {
	if Ratio.LessThanOrEqual(decimal.Zero) || Ratio.GreaterThan(decimal.NewFromInt(1)) {
		log.Printf("Invalid input '%s' as Delta to NewStairway()\n", Ratio)
		return nil
	}
	Ratio = decimal.NewFromInt(1).Add(decimal.NewFromInt(1).Sub(Ratio))
	var Sta Stairway
	Sta.Minimum = Minimum
	Sta.Ratio = Ratio
	if Minimum.Equals(decimal.Zero) {
		Minimum = cMinimum
	}
	Val := Minimum
	//Sta.stepsDec = append([]decimal.Decimal{cMaxStairstep}, Sta.stepsDec...)
	Sta.stepsDec = append(Sta.stepsDec, Minimum)
	F, _ := Minimum.Float64()
	//Sta.stepsF64 = append([]float64{F}, Sta.stepsF64...)
	Sta.stepsF64 = append(Sta.stepsF64, F)
	var cStep int
	for Val.LessThanOrEqual(cMaxStairstep) {
		cStep++
		NewVal := Val.Mul(Ratio).Round(8)
		if Val.Equals(NewVal) {
			NewVal = Val.Add(Minimum).Round(8)
		}
		//Sta.stepsDec = append([]decimal.Decimal{NewVal}, Sta.stepsDec...)
		Sta.stepsDec = append(Sta.stepsDec, NewVal)
		F, _ := NewVal.Float64()
		//Sta.stepsF64 = append([]float64{F}, Sta.stepsF64...)
		Sta.stepsF64 = append(Sta.stepsF64, F)
		Val = NewVal
	}
	return &Sta
}

func (Sta *Stairway) AllSteps() []decimal.Decimal {
	return Sta.stepsDec
}

func (Sta *Stairway) AllStepsF() []float64 {
	return Sta.stepsF64
}

func (Sta *Stairway) StepV(c int) decimal.Decimal {
	return Sta.stepsDec[c]
}

func (Sta *Stairway) StepVf(c int) float64 {
	return Sta.stepsF64[c]
}

func (Sta *Stairway) FindStepByVal(V decimal.Decimal) int {
	for k := range Sta.stepsDec {
		if V.LessThanOrEqual(Sta.stepsDec[k]) {
			return k
		}
	}
	return 0
}

func (Sta *Stairway) FindStepByValF64(V float64) int {
	for k := range Sta.stepsF64 {
		if V >= Sta.stepsF64[k] {
			return k
		}
	}
	return 0
}
