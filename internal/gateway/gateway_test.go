// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"testing"

	device "github.com/nereidai/nereidlink/sdk/device/go"
)

func TestResolveEdgeID(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "explicit EdgeID wins",
			cfg:  Config{CredentialID: "edge-nuc-token", EdgeID: "edge-nuc"},
			want: "edge-nuc",
		},
		{
			name: "empty EdgeID falls back to CredentialID",
			cfg:  Config{CredentialID: "edge-nuc-token", EdgeID: ""},
			want: "edge-nuc-token",
		},
		{
			name: "whitespace-only EdgeID falls back",
			cfg:  Config{CredentialID: "edge-nuc-token", EdgeID: "   "},
			want: "edge-nuc-token",
		},
		{
			name: "both empty yields empty",
			cfg:  Config{},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveEdgeID(tc.cfg); got != tc.want {
				t.Errorf("resolveEdgeID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildHelloAdvertisesSecretTypes(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{
		EnableSecrets: true,
		SecretTypeAdverts: []device.SecretTypeAdvert{
			{Type: "onvif-basic", TypeVersion: "1.0"},
		},
	}}
	hello := c.buildHello(nil)
	if len(hello.SecretTypes) != 1 || hello.SecretTypes[0].Type != "onvif-basic" {
		t.Fatalf("hello.SecretTypes = %+v", hello.SecretTypes)
	}
	found := false
	for _, f := range hello.Features.Optional {
		if f == featureSecrets {
			found = true
		}
	}
	if !found {
		t.Fatalf("'secrets' feature not advertised, optional=%v", hello.Features.Optional)
	}
}

func TestBuildHelloWithoutSecretsHasNoSecretTypes(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{
		SecretTypeAdverts: []device.SecretTypeAdvert{{Type: "onvif-basic", TypeVersion: "1.0"}},
	}}
	hello := c.buildHello(nil)
	if len(hello.SecretTypes) != 0 {
		t.Fatalf("secret types advertised without the feature: %+v", hello.SecretTypes)
	}
}

func TestBuildHelloProfileDownlinkOptIn(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{EnableProfiles: true}}
	hello := c.buildHello(nil)

	var found bool
	for _, f := range hello.Features.Optional {
		if f == device.FeatureProfiles {
			found = true
		}
	}
	if !found {
		t.Fatalf("'profiles' feature not in optional set: %v", hello.Features.Optional)
	}
	for _, f := range hello.Features.Required {
		if f == device.FeatureProfiles {
			t.Fatal("'profiles' must be optional, not required: old platforms must negotiate anyway")
		}
	}
	want := []string{device.ProfileFormatEdgeX, device.ProfileFormatNeutral}
	if len(hello.ProfileFormats) != len(want) ||
		hello.ProfileFormats[0] != want[0] || hello.ProfileFormats[1] != want[1] {
		t.Fatalf("profile_formats=%v, want %v (ordered)", hello.ProfileFormats, want)
	}
}

func TestBuildHelloWithoutProfilesOmitsFeature(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{}}
	hello := c.buildHello(nil)
	for _, f := range append(append([]string{}, hello.Features.Required...), hello.Features.Optional...) {
		if f == device.FeatureProfiles {
			t.Fatal("profiles feature advertised while disabled")
		}
	}
	if len(hello.ProfileFormats) != 0 {
		t.Fatalf("profile_formats advertised while disabled: %v", hello.ProfileFormats)
	}
}

func TestBuildHelloInferenceOptIn(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{
		EnableInference:   true,
		InferenceBackends: []string{"kserve", "openai"},
	}}
	hello := c.buildHello(nil)

	var found bool
	for _, f := range hello.Features.Optional {
		if f == device.FeatureInference {
			found = true
		}
	}
	if !found {
		t.Fatalf("'inference' feature not in optional set: %v", hello.Features.Optional)
	}
	for _, f := range hello.Features.Required {
		if f == device.FeatureInference {
			t.Fatal("'inference' must be optional, not required: old platforms must negotiate anyway")
		}
	}
	if hello.Inference == nil {
		t.Fatal("inference capability block missing with feature enabled")
	}
	if hello.Inference.Contract != device.InferenceContractModel1 {
		t.Fatalf("contract=%q, want %q", hello.Inference.Contract, device.InferenceContractModel1)
	}
	want := []string{"kserve", "openai"}
	if len(hello.Inference.Backends) != len(want) ||
		hello.Inference.Backends[0] != want[0] || hello.Inference.Backends[1] != want[1] {
		t.Fatalf("backends=%v, want %v (ordered)", hello.Inference.Backends, want)
	}

	// Evidence JPEGs use the MQTT binary media marker relay; it rides the
	// same inference opt-in but is itself optional.
	var hasBlobRelay bool
	for _, f := range hello.Features.Optional {
		if f == device.FeatureBlobRelay {
			hasBlobRelay = true
		}
	}
	if !hasBlobRelay {
		t.Fatalf("'blob-relay' feature not in optional set: %v", hello.Features.Optional)
	}
	for _, f := range hello.Features.Required {
		if f == device.FeatureBlobRelay {
			t.Fatal("blob-relay must be optional, not required")
		}
	}
}

func TestBuildHelloInferenceDefaultsToKserveAndOpenAI(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{EnableInference: true}}
	hello := c.buildHello(nil)
	want := []string{"kserve", "openai"}
	if hello.Inference == nil || len(hello.Inference.Backends) != len(want) ||
		hello.Inference.Backends[0] != want[0] || hello.Inference.Backends[1] != want[1] {
		t.Fatalf("inference block=%+v, want %v", hello.Inference, want)
	}
}

// TestBuildHelloInferenceAbsentNode omits the whole capability when NI is
// configured but the runtime probe says the manager device does not exist.
func TestBuildHelloInferenceAbsentNode(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{
		EnableInference:    true,
		InferenceAvailable: func() bool { return false },
	}}
	hello := c.buildHello(nil)
	if hello.Inference != nil {
		t.Fatalf("inference block advertised on a node without NI: %+v", hello.Inference)
	}
	for _, f := range append(append([]string{}, hello.Features.Required...), hello.Features.Optional...) {
		if f == device.FeatureInference || f == device.FeatureBlobRelay {
			t.Fatalf("%q advertised on a node without NI: %v", f, hello.Features.Optional)
		}
	}

	// Rising edge: the very next (re)hello with NI present advertises it.
	c.cfg.InferenceAvailable = func() bool { return true }
	hello = c.buildHello(nil)
	if hello.Inference == nil {
		t.Fatal("inference block missing after the NI presence rising edge")
	}
}

func TestBuildHelloWithoutInferenceOmitsFeature(t *testing.T) {
	c := &Client{edgeID: "edge-x", cfg: Config{}}
	hello := c.buildHello(nil)
	for _, f := range append(append([]string{}, hello.Features.Required...), hello.Features.Optional...) {
		if f == device.FeatureInference {
			t.Fatal("inference feature advertised while disabled")
		}
		if f == device.FeatureBlobRelay {
			t.Fatal("blob-relay must only be advertised on inference-enabled gateways")
		}
	}
	if hello.Inference != nil {
		t.Fatalf("inference block advertised while disabled: %+v", hello.Inference)
	}
}
