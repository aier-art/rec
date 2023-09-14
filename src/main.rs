use std::{
  cmp::{max, min},
  collections::{HashMap, HashSet},
  convert::AsRef,
};

use anyhow::Result;
use const_str::{concat, convert_ascii_case};
use gt::{Q, Q01, QE};
use intbin::{bin_u64, u64_bin};
use strum_macros::{AsRefStr, FromRepr};
use sts::ms;
use xkv::{
  conn,
  fred::interfaces::{HashesInterface, SetsInterface, SortedSetsInterface},
};

mod img;
pub const CID_IMG: i8 = 2;

const LIMIT: usize = {
  #[cfg(debug_assertions)]
  {
    66
  }

  #[cfg(not(debug_assertions))]
  {
    32768
  }
};

const LIMIT_F32: f32 = LIMIT as f32;
const HOUR: u64 = 3600 * 1000;
const 平均分: u64 = 20000;
const SCORE_BASE: u64 = 16;

conn!(AK = AK);
conn!(KV = KV);

apg::Q01! {
    is_adult: SELECT adult FROM bot.task WHERE id=$1;
}
apg::Q1! {
    seen_click_fav_ratio: SELECT seen*100/click,click*100/fav FROM log.seen_click_fav_sum;
}

apg::Q! {
    seen_click_fav: SELECT cid,rid,seen,click,fav FROM log.seen_click_fav WHERE hour>$1 AND cid>=$2 AND rid>$3 ORDER BY cid,rid LIMIT 16384
}

const 平均宽度: u64 = 858;

async fn update_score(begin_hour: u32) -> Result<()> {
  let (seen_click, click_fav) = seen_click_fav_ratio().await?;
  let mut pre_cid: i8 = 0;
  let mut pre_rid: u64 = 0;
  loop {
    let li = seen_click_fav(begin_hour, pre_cid, pre_rid).await?;
    if li.is_empty() {
      break;
    }
    let p = KV.pipeline();
    let (_pre_cid, _pre_rid, ..) = li.last().unwrap();
    pre_cid = *_pre_cid;
    pre_rid = *_pre_rid;

    let mut map: HashMap<i8, HashSet<_, _>, _> = HashMap::new();
    for (cid, rid, ..) in &li {
      let cid = *cid;
      let rid = *rid;
      if let Some(set) = map.get_mut(&cid) {
        set.insert(rid);
      } else {
        map.insert(cid, HashSet::from([rid]));
      }
    }

    let mut cid_rid_li = Vec::with_capacity(map.len());
    let mut whr_li_li = Vec::with_capacity(map.len());

    for (cid, rid_li_num) in map {
      let rid_li = rid_li_num.iter().map(|i| u64_bin(*i)).collect::<Vec<_>>();

      let whr_li: Vec<Option<Vec<u8>>> = AK.hmget(format!("whr{cid}"), rid_li).await?;
      whr_li_li.push(whr_li);

      cid_rid_li.push((cid, rid_li_num));
    }

    let mut whrs = HashMap::new();

    for ((cid, rid_li_num), whr_li) in cid_rid_li.into_iter().zip(whr_li_li) {
      let mut map = HashMap::new();
      for (rid, width) in rid_li_num.into_iter().zip(whr_li.into_iter().map(|i| {
        if let Some(i) = i {
          min(max(bin_u64(i), 310), 3100)
        } else {
          平均宽度
        }
      })) {
        map.insert(rid, width);
      }
      whrs.insert(cid, map);
    }

    for (cid, rid, seen, click, fav) in li {
      // ./score.py 可以调试分数

      let width = whrs.get(&cid).unwrap().get(&rid).unwrap();

      let 分子 =
        平均宽度 * ((100 * click + fav * click_fav) * seen_click + (SCORE_BASE * 平均分));
      let 分母 = seen * width + SCORE_BASE * 平均宽度;
      let score = 分子 / 分母;

      tracing::info!("{cid} {rid} click {click} fav {fav} seen {seen} : {分子} / {分母} = {score}");
      let score = max(1, score as u64); // 分数不能为 0， 因为前端很多依赖0作为分隔符

      if cid == CID_IMG {
        img::update(rid, score).await?;
      }
      if let Some(adult) = is_adult(rid).await? {
        // 开发服务器数据不全可能会有问题，线上也可能有黑客注入的脏数据
        let item = (score as f64, &vb::e([cid as u64, rid])[..]);
        let key = if adult > 0 { b"r0" } else { b"r1" };
        p.zadd(&key[..], None, None, false, false, item).await?;
        p.zadd(&b"r"[..], None, None, false, false, item).await?;
      }
    }
    p.all().await?;
  }
  Ok(())
}

// TODO 每个小时记录统计数据

#[derive(FromRepr, Debug, PartialEq, AsRefStr, Eq, Hash, Clone)]
#[repr(i8)]
pub enum Action {
  Seen = 1,
  Click = 2,
  Fav = 3,
  FavRm = 4,
}

macro_rules! len {
    () => { 0 };
    ($($i:tt $j:tt)*) => { len!($($i)*) * 2 };
    ($j:tt $($i:tt)*) => { len!($($i)*) + 1 };
}
macro_rules! const_arr {
    ($($name:ident),*) => {
        const ACTION_SIZE:usize = len!($($name)*);
        const ACTION: [&'static [u8]; ACTION_SIZE] = [
            $(concat!(convert_ascii_case!(upper_camel, stringify!($name)),":").as_bytes()),*
        ];
        const COLUMN: [&'static str; ACTION_SIZE] = [
            $(stringify!($name)),*
        ];
    };
}

const_arr!(seen, click, fav);

fn max_lt(target: u64, duration: u64) -> u64 {
  target - target % duration
}

/*
统计全局 收藏 点击 展示 的数
*/

pub async fn log_n(
  hour: u64,
  aid_cid_rid: &HashSet<(Action, i8, i64)>,
  seen_click_fav_n: &mut [HashMap<(i8, i64), u64>; 3],
) -> Result<()> {
  for li in aid_cid_rid.iter().collect::<Vec<_>>().chunks(LIMIT) {
    let p = AK.pipeline();
    for (action, cid, rid) in li {
      let key = vb::e([*cid as u64, *rid as _]);
      let key = &[action.as_ref().as_bytes(), b":", &key].concat()[..];
      p.scard(key).await?;
    }

    let n_li: Vec<u64> = p.all().await?;
    let mut to_insert = Vec::with_capacity(n_li.len());
    for (key, n) in li.iter().zip(n_li.iter()) {
      let (action, cid, rid) = key;
      let aid = action.clone() as i8;
      to_insert.push(format!("({hour},{aid},{cid},{rid},{n})"));
      seen_click_fav_n[(aid - 1) as usize].insert((*cid, *rid), *n);
    }
    let to_insert = to_insert.join(",");
    QE(
      format!("INSERT INTO log_n (ts,aid,cid,rid,n) VALUES {to_insert}"),
      &[],
    )
    .await?;
  }
  Ok(())
}

pub async fn to_bk(
  input_li: &[(
    i64, //ts
    i64, //uid
    i8,  //aid
    i8,  //cid
    i64, //rid
  )],
  aid_cid_rid: &mut HashSet<(Action, i8, i64)>,
) -> Result<()> {
  for li in input_li.chunks(LIMIT * 2) {
    let p = AK.pipeline();

    for (_ts, uid, aid, cid, rid) in li {
      let uid = u64_bin(*uid as u64);
      let aid = *aid;
      if let Some(action) = Action::from_repr(aid) {
        let key = vb::e([*cid as u64, *rid as _]);

        let action = if action == Action::FavRm {
          p.srem(&[ACTION[Action::Fav as usize - 1], &key].concat()[..], uid)
            .await?;
          Action::Fav
        } else {
          let pos = aid as usize - 1;
          p.sadd(&[ACTION[pos], &key].concat()[..], uid).await?;
          action
        };
        aid_cid_rid.insert((action, *cid, *rid));
      }
    }

    let _: Vec<()> = p.all().await?;
  }
  Ok(())
}

const HSET_ITER: &str = "iter";
const HSET_ITER_LOG_REC: &str = "logRec";

fn ts_hour(ts: u64) -> u64 {
  ts / HOUR - 1
}

const RERUN: &str = "RERUN";

async fn begin_id() -> Result<Option<Vec<u8>>> {
  if let Ok(rerun) = std::env::var(RERUN) {
    // 设置环境，可以全部重跑
    // let begin: Option<Vec<u8>> = None
    if !rerun.is_empty() {
      tracing::info!("{}", RERUN);
      return Ok(None);
    }
  }
  Ok(AK.hget(HSET_ITER, HSET_ITER_LOG_REC).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
  loginit::init();
  let max_end = max_lt(ms() - 60000, HOUR);

  let mut begin = match begin_id().await? {
    Some(begin) => bin_u64(begin),
    None => {
      let begin = Q01(
        "SELECT CAST(ts AS BIGINT) t FROM log ORDER BY ts LIMIT 1",
        &[],
      )
      .await?;
      if let Some(begin) = begin {
        let begin: i64 = begin.get(0);
        begin as _
      } else {
        return Ok(());
      }
    }
  };
  let begin_hour = ts_hour(begin);
  let mut step = 1000;
  let mut goon = true;

  let mut aid_cid_rid = HashSet::new();
  let mut seen_click_fav_n = [HashMap::new(), HashMap::new(), HashMap::new()];

  while goon {
    let mut end = begin + step;

    let real_step = if end / HOUR != begin / HOUR {
      let t = begin % HOUR;
      end = begin + HOUR - t;
      t
    } else {
      step
    };
    let end = if end >= max_end {
      goon = false;
      max_end
    } else {
      end
    };

    let sql = format!("SELECT CAST(ts AS BIGINT) t,uid,aid,cid,rid FROM log WHERE ts>=ARROW_CAST({begin},'Timestamp(Millisecond,None)') AND ts<ARROW_CAST({end},'Timestamp(Millisecond,None)') ORDER BY ts");
    let li: Vec<(i64, i64, i8, i8, i64)> = Q(sql, &[])
      .await?
      .into_iter()
      .map(|i| (i.get(0), i.get(1), i.get(2), i.get(3), i.get(4)))
      .collect();

    to_bk(&li, &mut aid_cid_rid).await?;

    let len = li.len();

    if len > 0 {
      let len = len as f32;
      step = max(
        (((real_step as f32) * (LIMIT_F32 / len) + ((32 * step) as f32)) / 33.0).round() as u64,
        1,
      );
    } else if real_step != step {
      step = min(step * 2, HOUR);
    };
    begin = end;

    if end % HOUR == 0 {
      let hour = ts_hour(end);
      log_n(hour, &aid_cid_rid, &mut seen_click_fav_n).await?;
      aid_cid_rid = HashSet::new();
    }
  }

  let end_hour = ts_hour(max_end);

  for (map, name) in seen_click_fav_n.iter().zip(COLUMN.iter()) {
    if !map.is_empty() {
      let mut to_insert = Vec::with_capacity(map.len());
      for ((cid, rid), n) in map {
        to_insert.push(format!("({cid},{rid},{n},{end_hour})"));
      }
      let to_insert = to_insert.join(",");
      let sql = format!("INSERT INTO log.seen_click_fav (cid,rid,{name},hour) VALUES {to_insert} ON CONFLICT (cid,rid) DO UPDATE SET {name}=log.seen_click_fav.{name}+EXCLUDED.{name},hour=EXCLUDED.hour");
      tracing::info!("{}", sql);
      apg::QE(sql, &[]).await?;
    }
  }

  update_score(begin_hour as _).await?;

  AK.hset(HSET_ITER, (HSET_ITER_LOG_REC, u64_bin(max_end)))
    .await?;
  Ok(())
}
