use anyhow::Result;
use clip_qdrant::qdrant_client;
use qdrant_client::{client::Payload, qdrant::PointId};

pub async fn update(id: u64, quality: u64) -> Result<()> {
  let id: PointId = id.into();
  let mut payload = Payload::new();
  payload.insert('q', quality as i64);
  qdrant_client()
    .set_payload("clip", &vec![id].into(), payload, None)
    .await?;
  Ok(())
}
