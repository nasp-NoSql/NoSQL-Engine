package countmin_sketch

import "math"

// CalculateW returns width for desired epsilon
func CalculateW(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

// CalculateD returns depth for desired delta
func CalculateD(delta float64) uint {
	return uint(math.Ceil(math.Log(1 / delta)))
}
