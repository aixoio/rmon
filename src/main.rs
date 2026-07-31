use std::time::Duration;

use chrono::{DateTime, Local};
use crossterm::event::{self, Event, EventStream, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use human_repr::HumanThroughput;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Alignment, Constraint, Layout},
    style::{Color, Style, Stylize},
    symbols::Marker,
    widgets::{Axis, Chart, Dataset, GraphType, Paragraph},
};
use sysinfo::Networks;
use tokio::{
    sync::mpsc::{self, Receiver},
    task, time,
};

enum Msg {
    Data {
        up: u64,
        down: u64,
        time: DateTime<Local>,
    },
    Crossterm(event::Event),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel(8);

    let tx1 = tx.clone();
    task::spawn(async move {
        let mut i = time::interval(Duration::from_millis(500));
        let mut nets = Networks::new();

        loop {
            i.tick().await;
            nets.refresh(true);
            let time = Local::now();

            let (up, down) = nets.iter().fold((0_u64, 0_u64), |(up, down), (_, net)| {
                (up + net.transmitted(), down + net.received())
            });

            // Network counters are measured over the 500 ms refresh interval.
            let up = up.saturating_mul(2);
            let down = down.saturating_mul(2);

            if tx1.send(Msg::Data { up, down, time }).await.is_err() {
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
    let mut app = App::new(rx);
    let res = app.run(&mut terminal).await;
    ratatui::restore();

    res
}

struct App {
    rx: Receiver<Msg>,

    running: bool,
    data: Vec<(u64, u64, DateTime<Local>)>,
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
                Msg::Data { up, down, time } => {
                    self.update_data(up, down, time);

                    terminal.draw(|f| self.draw(f))?;
                }
            }
        }

        Ok(())
    }

    fn update_data(&mut self, up: u64, down: u64, time: DateTime<Local>) {
        if self.data.is_empty() {
            self.data_min = (up, down);
            self.data_max = (up, down);
            self.data.push((up, down, time));
            return;
        }

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

        self.data.push((up, down, time));
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

    fn draw(&self, frame: &mut Frame) {
        let upload_data: Vec<(f64, f64)> = self
            .data
            .iter()
            .map(|d| (d.2.timestamp_millis() as f64, d.0 as f64))
            .collect();

        let dataset_up = Dataset::default()
            .name("Upload")
            .marker(Marker::Dot)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&upload_data);

        let download_data: Vec<(f64, f64)> = self
            .data
            .iter()
            .map(|d| (d.2.timestamp_millis() as f64, d.1 as f64))
            .collect();

        let dataset_down = Dataset::default()
            .name("Download")
            .marker(Marker::Dot)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Cyan))
            .data(&download_data);

        let x_bounds = [
            self.data.first().unwrap().2.timestamp_millis() as f64,
            self.data.last().unwrap().2.timestamp_millis() as f64 + 1.0,
        ];
        let areas = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .split(frame.area());
        let current = self.data.last().unwrap();
        let status = Paragraph::new(format!(
            "Upload: {}   Download: {}",
            current.0.human_throughput_bytes(),
            current.1.human_throughput_bytes(),
        ))
        .alignment(Alignment::Center);

        let upload_chart = Chart::new(vec![dataset_up])
            .x_axis(Axis::default().title("Time".blue()).bounds(x_bounds))
            .y_axis(
                Axis::default()
                    .title("Upload (B/s)".green())
                    .bounds([self.data_min.0 as f64, self.data_max.0 as f64 + 1.0]),
            );
        let download_chart = Chart::new(vec![dataset_down])
            .x_axis(Axis::default().title("Time".blue()).bounds(x_bounds))
            .y_axis(
                Axis::default()
                    .title("Download (B/s)".cyan())
                    .bounds([self.data_min.1 as f64, self.data_max.1 as f64 + 1.0]),
            );

        frame.render_widget(status, areas[0]);
        frame.render_widget(upload_chart, areas[1]);
        frame.render_widget(download_chart, areas[2]);
    }
}
