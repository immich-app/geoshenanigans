# The state holds the storage zones' passwords and the middleware script's deployment key (provider 0.20.2 keeps them
# as attributes), and its backend credential (tf/tf_state) is every pull request's too, so the state is encrypted with a
# passphrase in tf_bunny, which only this unit's protected jobs read (README.md). The backend holds ciphertext no other
# job can read, and AES-GCM refuses a state written without the passphrase. Enforced: no state or plan is ever written
# unencrypted.
terraform {
  encryption {
    key_provider "pbkdf2" "state" {
      passphrase = var.state_passphrase
    }

    method "aes_gcm" "state" {
      keys = key_provider.pbkdf2.state
    }

    state {
      method   = method.aes_gcm.state
      enforced = true
    }

    plan {
      method   = method.aes_gcm.state
      enforced = true
    }
  }
}
