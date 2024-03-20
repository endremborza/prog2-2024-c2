use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver},
        Arc,
    },
    thread,
};

use csv::{ReaderBuilder, Writer};
use rouille::Response;
use serde::Deserialize;

type FT = f32;
const CS: usize = 3;
const LEAF_SIZE: usize = 64;

type ArrT = [FT; CS];
type QT = [FT; CS + 2];

fn dist(query: &QT, point: &ArrT) -> FT {
    if query[3] > point[2] || query[4] < point[2] {
        return FT::INFINITY;
    }
    return (query[0] - point[0]).powf(2.0) + (query[1] - point[1]).powf(2.0);
}

fn dist_onedim(query: &QT, point: &ArrT, dim: usize) -> FT {
    let v = point[dim];
    if dim == 2 {
        if query[3] > v || query[4] < v {
            return FT::INFINITY;
        }
        return 0.0;
    }
    (query[dim] - v).powf(2.0)
}

#[derive(Deserialize, Debug)]
struct Hit {
    pub x: FT,
    pub y: FT,
    pub dmg: FT,
    pub dmg_type: String,
}

#[derive(Deserialize, Debug)]
struct Q {
    pub x: FT,
    pub y: FT,
    pub dmg_min: FT,
    pub dmg_max: FT,
}

struct Entry {
    ind: usize,
    arr: [FT; CS],
}

enum MyTree {
    Split {
        dim: usize,
        left: Box<MyTree>,
        right: Box<MyTree>,
        entry: Entry,
    },
    Leaf {
        entries: Vec<Entry>,
    },
}

impl MyTree {
    fn new(mut entries: Vec<Entry>, start_dim: usize) -> Self {
        if entries.len() <= LEAF_SIZE {
            return Self::Leaf { entries };
        }
        entries.sort_by(|a, b| a.arr[start_dim].partial_cmp(&b.arr[start_dim]).unwrap());
        let mut right_entries = Vec::with_capacity(entries.len() / 2);
        for _ in 1..entries.len() / 2 {
            right_entries.push(entries.pop().unwrap());
        }
        let entry = entries.pop().unwrap();
        entries.shrink_to_fit();
        let new_dim = (start_dim + 1) % CS;
        Self::Split {
            dim: start_dim,
            left: Box::new(Self::new(entries, new_dim)),
            right: Box::new(Self::new(right_entries, new_dim)),
            entry,
        }
    }

    fn query(&self, query: &QT) -> (usize, FT) {
        match self {
            Self::Leaf { entries } => best_entry(entries, query),
            Self::Split {
                dim: _,
                left: _,
                right: _,
                entry: _,
            } => self.query_recurse(query, FT::INFINITY),
        }
    }

    fn query_recurse(&self, query: &QT, best_dist: FT) -> (usize, FT) {
        match self {
            Self::Leaf { entries } => best_entry(entries, query),
            Self::Split {
                dim,
                left,
                right,
                entry,
            } => {
                let self_dist = dist(query, &entry.arr);
                let (q_side, other_side) = {
                    if query[*dim] < entry.arr[*dim] {
                        (left, right)
                    } else {
                        (right, left)
                    }
                };
                let onedim_dist = dist_onedim(&query, &entry.arr, *dim);
                let one_to_beat = best_dist.min(self_dist);
                let (qside_ind, qside_dist) = q_side.query_recurse(query, one_to_beat);
                let best_w_q = one_to_beat.min(qside_dist);
                if onedim_dist < best_w_q {
                    let (otherside_ind, otherside_dist) = other_side.query_recurse(query, best_w_q);
                    if otherside_dist < best_w_q {
                        return (otherside_ind, otherside_dist);
                    }
                }
                if qside_dist < one_to_beat {
                    return (qside_ind, qside_dist);
                }
                if self_dist < best_dist {
                    return (entry.ind, self_dist);
                }
                (0, FT::INFINITY)
            }
        }
    }
}

fn best_entry(entries: &Vec<Entry>, query: &QT) -> (usize, FT) {
    let mut out_dist = FT::INFINITY;
    let mut ind = 0;
    for e in entries {
        let new_d = dist(&query, &e.arr);
        if new_d < out_dist {
            out_dist = new_d;
            ind = e.ind;
        }
    }

    (ind, out_dist)
}

fn main() {
    let mut rdr = ReaderBuilder::new().from_path("input.csv").unwrap();
    let mut entry_map = HashMap::new();
    let mut dmg_map = HashMap::new();

    for hitr in rdr.deserialize::<Hit>() {
        let hit = hitr.unwrap();
        let dmg_type = hit.dmg_type;
        let dmgs = dmg_map.entry(dmg_type.clone()).or_insert(vec![]);
        let entries = entry_map.entry(dmg_type.clone()).or_insert(vec![]);
        dmgs.push(hit.dmg);
        entries.push(Entry {
            ind: entries.len(),
            arr: [hit.x, hit.y, hit.dmg],
        });
    }

    let mut query_channels = HashMap::new();
    let mut result_channels = HashMap::new();
    let mut ping_channels = vec![];

    for (k, entry_vec) in entry_map {
        let (qtx, qrx) = mpsc::channel();
        let (rtx, rrx) = mpsc::channel();
        let (ptx, prx) = mpsc::channel();
        ping_channels.push(prx);
        query_channels.insert(k.clone(), qtx);
        result_channels.insert(k.clone(), rrx);
        let dmgs = Arc::new(dmg_map[&k].clone());
        thread::spawn(move || qtree(entry_vec, dmgs, rtx, qrx, ptx));
    }

    // let (ping_tx, ping_rx) = mpsc::channel();
    let (ping_tx, ping_rx) = mpsc::sync_channel(1);

    println!("collecting pings");
    for rec in ping_channels {
        rec.recv().unwrap();
        println!("ping");
    }
    println!("all pings");

    thread::spawn(move || handle_result(result_channels, ping_rx));
    println!("started res thread");
    // handle_request(query_channels, ping_tx);
    rouille::start_server("localhost:5678", move |_| {
        handle_request(query_channels.clone(), ping_tx.clone())
    });
}
fn qtree(
    entry_vec: Vec<Entry>,
    dmgs: Arc<Vec<FT>>,
    res: mpsc::Sender<Vec<String>>,
    qrx: mpsc::Receiver<Vec<QT>>,
    ptx: mpsc::Sender<()>,
) {
    let tree = MyTree::new(entry_vec, 2);
    ptx.send(()).unwrap();
    loop {
        match qrx.recv() {
            Ok(qs) => {
                let mut out = vec![];
                for q in qs {
                    let res = tree.query(&q);
                    let mut dmg: String = "0".to_string();
                    if res.1 < FT::INFINITY {
                        dmg = dmgs[res.0].to_string();
                    }
                    out.push(dmg);
                }
                res.send(out).unwrap();
            }
            // Err(e) => println!("failed q receive {:?}", e),
            Err(_) => (),
        }
    }
}

fn handle_result(
    result_channels: HashMap<String, mpsc::Receiver<Vec<String>>>,
    rcv_ping: Receiver<()>,
) {
    println!("making writer");
    let mut writer = Writer::from_path("out.csv").unwrap();
    let header: Vec<String> = result_channels.keys().map(|e| e.to_string()).collect();
    writer.write_record(header.iter()).unwrap();
    println!("wrote headers");
    loop {
        let mut cols = vec![];
        for k in &header {
            cols.push(result_channels[k].recv().unwrap());
        }
        for i in 0..cols[0].len() {
            writer
                .write_record(cols.iter().map(|c| c[i].clone()))
                .unwrap();
        }

        writer.flush().unwrap();
        rcv_ping.recv().unwrap();
        rcv_ping.recv().unwrap();
    }
}

fn handle_request(
    query_channels: HashMap<String, mpsc::Sender<Vec<QT>>>,
    ping_sender: mpsc::SyncSender<()>,
) -> Response {
    let mut qs = vec![];
    for qr in ReaderBuilder::new()
        .from_path("query.csv")
        .unwrap()
        .deserialize::<Q>()
    {
        let q = qr.unwrap();
        let mid_dmg = (q.dmg_min + q.dmg_max) / 2.0;
        qs.push([q.x, q.y, mid_dmg, q.dmg_min, q.dmg_max]);
    }
    for k in query_channels.keys() {
        query_channels[k].send(qs.clone()).unwrap();
    }
    match ping_sender.send(()) {
        Ok(e) => println!("sent ping {:?}", e),
        Err(e) => println!("failed ping {:?}", e),
    }
    match ping_sender.send(()) {
        Ok(e) => println!("sent ping again {:?}", e),
        Err(e) => println!("failed ping 2 {:?}", e),
    }
    Response::empty_204()
}
