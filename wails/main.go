package main

import (
	"embed"
	"log"
	"net/url"
	"os"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	app := NewApp()

	targetURL := "https://www.devstudio.live"
	if envURL := os.Getenv("DEVSTUDIO_TARGET_URL"); envURL != "" {
		targetURL = envURL
	}
	target, _ := url.Parse(targetURL)
	handler := NewProxyHandler(target, assets)

	err := wails.Run(&options.App{
		Title:     "DevStudio",
		Width:     1280,
		Height:    800,
		MinWidth:  800,
		MinHeight: 600,
		AssetServer: &assetserver.Options{
			Handler: handler,
		},
		BackgroundColour: &options.RGBA{R: 15, G: 23, B: 42, A: 1},
		OnStartup:        app.startup,
		OnShutdown:       app.shutdown,
		Bind: []interface{}{
			app,
		},
	})
	if err != nil {
		log.Fatal("Error:", err.Error())
	}
}
