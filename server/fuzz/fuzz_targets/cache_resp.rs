#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| nabedra_fuzz::fuzz_cache_resp(data));
