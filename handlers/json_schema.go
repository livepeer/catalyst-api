package handlers

import "github.com/xeipuuv/gojsonschema"

var inputSchemas map[string]string = map[string]string{
	"TranscodeSegment": TranscodeSegmentRequestSchemaDefinition,
	"UploadVOD":        UploadVODRequestSchemaDefinition,
}

func compileJsonSchemas() map[string]*gojsonschema.Schema {
	compiled := make(map[string]*gojsonschema.Schema, 0)
	for name, text := range inputSchemas {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(text))
		if err != nil {
			// rase panic on program start
			panic(err) // fix schema text
		}
		compiled[name] = schema
	}
	return compiled
}

// Run compile step on program start:
var inputSchemasCompiled map[string]*gojsonschema.Schema = compileJsonSchemas()
