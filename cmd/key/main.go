package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/yourorg/sandboxing/internal/store"
	"os"
)

func main() {
	tenant := flag.String("tenant", "", "tenant identifier")
	revoke := flag.String("revoke-hash", "", "revoke an existing SHA-256 API key hash")
	flag.Parse()
	db, err := store.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx := context.Background()
	if err = db.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *revoke != "" {
		_, err = db.Raw().ExecContext(ctx, "update api_keys set revoked=true where key_hash=$1", *revoke)
	} else {
		var raw [32]byte
		if _, err = rand.Read(raw[:]); err == nil {
			key := base64.RawURLEncoding.EncodeToString(raw[:])
			err = db.ProvisionAPIKey(ctx, *tenant, key)
			if err == nil {
				fmt.Println(key)
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
