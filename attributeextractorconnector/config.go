package attributeextractorconnector

type Config struct {
	// AddAttributes will add metric attributes to the output metrics. Only string attributes are supported. If you need other types, consider the attribute or transform processors
	AddAttributes []AttributeExtractorAttribute `mapstructure:"add_attributes"`
	// ExcludeAttributeKeys will make the connector ignore the given attribute keys
	ExcludeAttributeKeys []string `mapstructure:"exclude_attributes"`
}

type AttributeExtractorAttribute struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

func (c *Config) Validate() error {
	return nil
}
