use qubic::crypto::{k12::k12_32, schnorrq};
use qubic::messages::{Computors, Tick};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("{}/../../fixtures/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

#[test]
fn check_vote_signature_variants() {
    let comp = Computors::decode(&fixture("computors.bin")[8..]).unwrap();
    let tick = Tick::decode(&fixture("quorum_tick.bin")[8..]).unwrap();
    println!("epoch {} idx {} tick {}", comp.epoch, tick.computor_index, tick.tick);
    let key = comp.public_keys[tick.computor_index as usize];
    let base = tick.encode();
    for x in 0u16..16 {
        let mut bytes = base;
        bytes[..2].copy_from_slice(&(tick.computor_index ^ x).to_le_bytes());
        let d = k12_32(&bytes[..288]);
        if schnorrq::verify_digest(&key, &d, &tick.signature) {
            println!("MATCH xor {x}");
        }
    }
    // Also try hashing with signature excluded, and hashing full.
    println!("raw full {}", schnorrq::verify_digest(&key, &k12_32(&base), &tick.signature));
    println!("raw pre {}", schnorrq::verify_digest(&key, &k12_32(&base[..288]), &tick.signature));
}
