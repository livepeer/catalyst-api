package handlers

import (
	"embed"
	"path/filepath"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

//go:embed schemas/*
var schemasDir embed.FS

func compileJsonSchemas() map[string]*gojsonschema.Schema {
	compiled := make(map[string]*gojsonschema.Schema, 0)
	inputSchemas, err := schemasDir.ReadDir("schemas")

	if err != nil {
		panic(err)
	}

	for _, file := range inputSchemas {
		text, err := schemasDir.ReadFile("schemas/" + file.Name())

		if err != nil {
			panic(err)
		}

		schema, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(text))
		if err != nil {
			// rase panic on program start
			panic(err) // fix schema text
		}

		name := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
		compiled[name] = schema
	}

	return compiled
}

// Run compile step on program start:
var inputSchemasCompiled map[string]*gojsonschema.Schema = compileJsonSchemas()
