package control

import "embed"

//go:embed assets/*
var assets embed.FS

func mustLoadAsset(name string) string {
	b, err := assets.ReadFile(name)
	if err != nil {
		panic(err)
	}
	return string(b)
}
