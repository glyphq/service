//! Computor sidecar key and helper behaviour.

use archive::{FileArchive, computor_key, get_computors, put_computors};

#[tokio::test]
async fn put_and_get_computors_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(root.path());
    let bytes = vec![0xabu8; 21_760];

    let key = put_computors(&archive, 240, &bytes).await.unwrap();
    assert_eq!(key, computor_key(240));
    assert_eq!(key, "raw/epoch=240/computors.bin");
    assert!(root.path().join(&key).is_file());

    let stored = get_computors(&archive, 240).await.unwrap();
    assert_eq!(stored.as_deref(), Some(bytes.as_slice()));
}

#[tokio::test]
async fn missing_computors_are_none() {
    let root = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(root.path());
    assert_eq!(get_computors(&archive, 7).await.unwrap(), None);
}

#[tokio::test]
async fn rewriting_an_epoch_replaces_the_sidecar() {
    let root = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(root.path());

    put_computors(&archive, 1, b"first").await.unwrap();
    put_computors(&archive, 1, b"second").await.unwrap();

    assert_eq!(
        get_computors(&archive, 1).await.unwrap().as_deref(),
        Some(b"second".as_slice())
    );
}

#[tokio::test]
async fn sidecars_are_scoped_by_epoch() {
    let root = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(root.path());

    put_computors(&archive, 1, b"one").await.unwrap();
    put_computors(&archive, 2, b"two").await.unwrap();

    assert_eq!(
        get_computors(&archive, 1).await.unwrap().as_deref(),
        Some(b"one".as_slice())
    );
    assert_eq!(
        get_computors(&archive, 2).await.unwrap().as_deref(),
        Some(b"two".as_slice())
    );
    assert_eq!(get_computors(&archive, 3).await.unwrap(), None);
}
