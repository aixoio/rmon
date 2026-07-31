use std::time::Duration;

use sysinfo::Networks;
use tokio::{sync::mpsc, task, time};

enum Msg {
    Data { up: u64, down: u64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8);

    let fetcher = task::spawn(async move {
        let mut i = time::interval(Duration::from_millis(500));
        let mut nets = Networks::new();

        loop {
            i.tick().await;
            nets.refresh(true);

            let (up, down) = nets.iter().fold((0_u64, 0_u64), |(up, down), (_, net)| {
                (up + net.transmitted(), down + net.received())
            });

            if tx.send(Msg::Data { up, down }).await.is_err() {
                break;
            }
        }
    });

    while let Some(Msg::Data { up, down }) = rx.recv().await {
        println!("{up} \t {down}");
    }

    fetcher.await?;

    Ok(())
}
