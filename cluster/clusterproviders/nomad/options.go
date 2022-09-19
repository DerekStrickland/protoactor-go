package nomad

import (
	v1 "github.com/hashicorp/nomad-openapi/v1"
)

type Option func(p *Provider)

func WithClientConfig(cfg *v1.ClientConfig) Option {
	return func(p *Provider) {
		if cfg.Address != "" {
			p.clientConfig.Address = cfg.Address
		}
		if cfg.CACert != "" {
			p.clientConfig.CACert = cfg.CACert
		}
		if cfg.ClientCert != "" {
			p.clientConfig.ClientCert = cfg.ClientCert
		}
		if cfg.ClientKey != "" {
			p.clientConfig.ClientKey = cfg.ClientKey
		}
		if cfg.Namespace != "" {
			p.clientConfig.Namespace = cfg.Namespace
		}
		if cfg.Region != "" {
			p.clientConfig.Region = cfg.Region
		}
		if cfg.TLSServerName != "" {
			p.clientConfig.TLSServerName = cfg.TLSServerName
		}
		if cfg.Token != "" {
			p.clientConfig.Token = cfg.Token
		}

		p.clientConfig.TLSSkipVerify = cfg.TLSSkipVerify
	}
}
