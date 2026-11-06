package aggregated

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"
	generatedopenapi "github.com/openshift-online/gecko/orlop/pkg/generated/openapi"

	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"

	"sigs.k8s.io/yaml"
)

// buildOpenAPIDefinitions creates a GetOpenAPIDefinitions function that merges
// generated apimachinery + API type definitions with definitions derived from
// each resource's structural schema.
func buildOpenAPIDefinitions(scheme *runtime.Scheme, resources []types.ResourceInfo) openapicommon.GetOpenAPIDefinitions {
	return func(ref openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
		defs := generatedopenapi.GetOpenAPIDefinitions(ref)

		for _, res := range resources {
			schema, err := schemaYAMLToOpenAPI(res.SchemaYAML)
			if err != nil {
				continue
			}
			if _, ok := schema.Properties["metadata"]; ok {
				schema.Properties["metadata"] = spec.Schema{
					SchemaProps: spec.SchemaProps{
						Ref: ref(metav1.ObjectMeta{}.OpenAPIModelName()),
					},
				}
			}

			gvkExtension := map[string]interface{}{
				"group":   res.GVK.Group,
				"version": res.GVK.Version,
				"kind":    res.GVK.Kind,
			}
			schema.AddExtension("x-kubernetes-group-version-kind", []interface{}{gvkExtension})

			obj, err := scheme.New(res.GVK)
			if err != nil {
				continue
			}
			definition := openapicommon.OpenAPIDefinition{Schema: *schema}
			if _, ok := schema.Properties["metadata"]; ok {
				definition.Dependencies = []string{metav1.ObjectMeta{}.OpenAPIModelName()}
			}
			defs[goTypeName(obj)] = definition

			listGVK := res.GVK.GroupVersion().WithKind(res.GVK.Kind + "List")
			listObj, err := scheme.New(listGVK)
			if err == nil {
				listSchema := &spec.Schema{}
				listSchema.Type = spec.StringOrArray{"object"}
				listSchema.Description = fmt.Sprintf("List of %s", res.GVK.Kind)
				defs[goTypeName(listObj)] = openapicommon.OpenAPIDefinition{
					Schema: *listSchema,
				}
			}
		}

		return defs
	}
}

func goTypeName(obj runtime.Object) string {
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
}

func schemaYAMLToOpenAPI(schemaYAML string) (*spec.Schema, error) {
	var propsV1 apiextv1.JSONSchemaProps
	if err := yaml.Unmarshal([]byte(schemaYAML), &propsV1); err != nil {
		return nil, err
	}

	return jsonSchemaPropsToOpenAPI(&propsV1), nil
}

func jsonSchemaPropsToOpenAPI(props *apiextv1.JSONSchemaProps) *spec.Schema {
	schema := &spec.Schema{}
	schema.Type = spec.StringOrArray{props.Type}

	if props.Description != "" {
		schema.Description = props.Description
	}

	if props.Properties != nil {
		schema.Properties = make(map[string]spec.Schema)
		for name, prop := range props.Properties {
			schema.Properties[name] = *jsonSchemaPropsToOpenAPI(&prop)
		}
	}

	if props.Items != nil && props.Items.Schema != nil {
		schema.Items = &spec.SchemaOrArray{
			Schema: jsonSchemaPropsToOpenAPI(props.Items.Schema),
		}
	}

	if len(props.Required) > 0 {
		schema.Required = props.Required
	}

	if props.Default != nil {
		schema.Default = props.Default.Raw
	}

	if props.Format != "" {
		schema.Format = props.Format
	}

	if len(props.Enum) > 0 {
		enums := make([]interface{}, len(props.Enum))
		for i, e := range props.Enum {
			var val interface{}
			if err := json.Unmarshal(e.Raw, &val); err == nil {
				enums[i] = val
			}
		}
		schema.Enum = enums
	}

	if props.Pattern != "" {
		schema.Pattern = props.Pattern
	}

	if props.Minimum != nil {
		schema.Minimum = props.Minimum
	}

	if props.Maximum != nil {
		schema.Maximum = props.Maximum
	}

	if props.MinLength != nil {
		schema.MinLength = props.MinLength
	}

	if props.MaxLength != nil {
		schema.MaxLength = props.MaxLength
	}

	if props.MinItems != nil {
		schema.MinItems = props.MinItems
	}

	if props.MaxItems != nil {
		schema.MaxItems = props.MaxItems
	}

	if props.AdditionalProperties != nil && props.AdditionalProperties.Schema != nil {
		schema.AdditionalProperties = &spec.SchemaOrBool{
			Schema: jsonSchemaPropsToOpenAPI(props.AdditionalProperties.Schema),
			Allows: true,
		}
	}

	if props.Nullable {
		schema.Nullable = true
	}

	// Composition keywords
	if len(props.OneOf) > 0 {
		schemas := make([]spec.Schema, len(props.OneOf))
		for i := range props.OneOf {
			schemas[i] = *jsonSchemaPropsToOpenAPI(&props.OneOf[i])
		}
		schema.OneOf = schemas
	}
	if len(props.AnyOf) > 0 {
		schemas := make([]spec.Schema, len(props.AnyOf))
		for i := range props.AnyOf {
			schemas[i] = *jsonSchemaPropsToOpenAPI(&props.AnyOf[i])
		}
		schema.AnyOf = schemas
	}
	if len(props.AllOf) > 0 {
		schemas := make([]spec.Schema, len(props.AllOf))
		for i := range props.AllOf {
			schemas[i] = *jsonSchemaPropsToOpenAPI(&props.AllOf[i])
		}
		schema.AllOf = schemas
	}
	if props.Not != nil {
		schema.Not = jsonSchemaPropsToOpenAPI(props.Not)
	}

	// x-kubernetes-* extensions
	if props.XPreserveUnknownFields != nil && *props.XPreserveUnknownFields {
		schema.AddExtension("x-kubernetes-preserve-unknown-fields", true)
	}
	if len(props.XListMapKeys) > 0 {
		schema.AddExtension("x-kubernetes-list-map-keys", props.XListMapKeys)
	}
	if props.XListType != nil {
		schema.AddExtension("x-kubernetes-list-type", *props.XListType)
	}
	if props.XMapType != nil {
		schema.AddExtension("x-kubernetes-map-type", *props.XMapType)
	}
	if props.XIntOrString {
		schema.AddExtension("x-kubernetes-int-or-string", true)
	}
	if len(props.XValidations) > 0 {
		schema.AddExtension("x-kubernetes-validations", props.XValidations)
	}
	if props.XEmbeddedResource {
		schema.AddExtension("x-kubernetes-embedded-resource", true)
	}

	return schema
}
