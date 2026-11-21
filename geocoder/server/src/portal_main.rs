// query-portal: the test portal's server (portal.rs). It syncs the whole
// planet build from the bucket by itself and serves each selection of it.
// Its tests run in query-server's (portal/tests.rs), beside the routes whose
// answers they compare its own with.

// The portal checks keys and rates; the dashboard and its logins are
// query-server's.
#[allow(dead_code)]
mod auth;
// The portal takes the downloader's resolve_files and its fetches of a file
// or a transport unit, not its region downloads.
#[allow(dead_code)]
mod downloader;
mod handlers;
mod portal;

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
