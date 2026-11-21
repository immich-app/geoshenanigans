// query-portal: the test portal's server (portal.rs). It syncs the whole
// planet build from the bucket by itself and serves each selection of it.
// Its tests run in query-server's (portal/tests.rs), beside the routes whose
// answers they compare its own with. It runs on Unix alone (the inodes its
// builds' links are checked by, the free space it weighs): elsewhere it
// says so and exits.

// The portal checks keys and rates; the dashboard and its logins are
// query-server's.
#[cfg(unix)]
#[allow(dead_code)]
mod auth;
// The portal takes the downloader's resolve_files and its fetches of a file
// or a transport unit, not its region downloads.
#[cfg(unix)]
#[allow(dead_code)]
mod downloader;
#[cfg(unix)]
mod handlers;
#[cfg(unix)]
mod portal;

#[cfg(not(unix))]
fn main() {
    eprintln!("Error: query-portal runs on Unix only");
    std::process::exit(1);
}

#[cfg(unix)]
fn main() {
    // Lookups run on blocking threads and the sync on a runtime of its own
    // (portal.rs): a few workers take the requests.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(portal::main());
}
