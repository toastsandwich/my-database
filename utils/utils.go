package utils

func Assert(condition bool) {
	if !condition {
		panic("assert fail panicing ...")
	}
}
