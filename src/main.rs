use std::time::Duration;

use crossterm::event::{self, Event, EventStream, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::{DefaultTerminal, Terminal};
use sysinfo::Networks;
use tokio::{
    sync::mpsc::{self, Receiver},
    task, time,
};

enum Msg {
    Data { up: u64, down: u64 },
    Crossterm(event::Event),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8);

    let tx1 = tx.clone();
    task::spawn(async move {
        let mut i = time::interval(Duration::from_millis(500));
        let mut nets = Networks::new();

        loop {
            i.tick().await;
            nets.refresh(true);

            let (up, down) = nets.iter().fold((0_u64, 0_u64), |(up, down), (_, net)| {
                (up + net.transmitted(), down + net.received())
            });

            if tx1.send(Msg::Data { up, down }).await.is_err() {
                break;
            }
        }
    });

    task::spawn(async move {
        let mut events = EventStream::new();

        while let Some(Ok(event)) = events.next().await {
            if tx.send(Msg::Crossterm(event)).await.is_err() {
                break;
            }
        }
    });

    let mut terminal = ratatui::init();
    let app = App::new(rx);
    let res = app.run(&mut terminal).await;
    ratatui::restore();

    res
}

struct App {
    rx: Receiver<Msg>,

    running: bool,
    data: Vec<(u64, u64)>,
    data_min: (u64, u64),
    data_max: (u64, u64),
}

impl App {
    fn new(rx: Receiver<Msg>) -> Self {
        App {
            rx,
            running: true,
            data: vec![],
            data_max: (0, 0),
            data_min: (0, 0),
        }
    }

    #[inline]
    fn running(&self) -> bool {
        self.running
    }

    async fn run(&mut self, terminal: &mut DefaultTerminal) -> anyhow::Result<()> {
        while self.running() {
            let Some(msg) = self.rx.recv().await else {
                break;
            };

            match msg {
                Msg::Crossterm(e) => self.handle_events(e)?,
                Msg::Data { up, down } => self.update_data(up, down),
            }
        }

        Ok(())
    }

    fn update_data(&mut self, up: u64, down: u64) {
        let (umin, dmin) = self.data_min;

        if umin > up {
            self.data_min.0 = up;
        }

        if dmin > down {
            self.data_min.1 = down;
        }

        let (umax, dmax) = self.data_max;

        if umax < up {
            self.data_max.0 = up;
        }

        if dmax < down {
            self.data_max.1 = down;
        }

        self.data.push((up, down));
    }

    fn handle_events(&mut self, event: Event) -> anyhow::Result<()> {
        match event {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                match key_event.code {
                    event::KeyCode::Char('c') => {
                        if key_event.modifiers.contains(KeyModifiers::CONTROL) {
                            self.running = false;
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        Ok(())
    }
}
