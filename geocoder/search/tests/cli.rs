use std::process::Command;

#[test]
fn a_flag_given_last_without_its_value_is_a_usage_error() {
    for args in [["build", "set", "index", "--sub"], ["query", "set", "index", "--limit"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_place-search")).args(args).output().expect("run place-search");
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        assert_eq!(String::from_utf8_lossy(&out.stderr), format!("{} needs a value\n", args[3]), "{args:?}");
    }
}
