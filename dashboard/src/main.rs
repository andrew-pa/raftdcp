use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures::prelude::*;
use termion::raw::IntoRawMode;
use std::sync::RwLock;
use tui::Terminal;
use tui::backend::TermionBackend;
use tui::widgets::{Widget, Block, Borders, BarChart, Chart, Dataset, GraphType, Axis, Paragraph};
use tui::layout::{Layout, Constraint, Direction};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans, Text};

use raft_proto::*;

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or("./cluster.json".into());
    let cluster = ClusterConfig::from_disk(config_path, Uuid::new_v4()).await.unwrap();

    let stdout = std::io::stdout().into_raw_mode().unwrap();
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.clear().unwrap();
    loop {
        let states: Vec<(&NodeId, Result<ServerDebugReport>)> = future::join_all(cluster.addresses.iter()
            .filter(|(id, _)| cluster.self_id != **id)
            .map(|(id, _)| cluster.get_client(id).then(move |c| async move {
                (id, match c {
                    Ok(cl) => cl.debug_report(tarpc::context::current()).await.map_err(Into::into),
                    Err(e) => Err(e)
                })
            }).boxed())).await;

        let term_size = terminal.size().unwrap();

        for (id, _) in states.iter().filter(|(_, r)| r.is_err()) {
            cluster.reset_client(id).await;
        }

        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(if term_size.width > term_size.height { Direction::Horizontal } else { Direction::Vertical })
                .constraints(states.iter().map(|_| Constraint::Ratio(1, states.len() as u32)).collect::<Vec<_>>())
                .split(f.size());
            // dbg!(&chunks);
            for (ix, (id, report)) in states.iter().enumerate() {
                let mut report_style = Style::default();
                if report.is_err() {
                    report_style = report_style.fg(Color::Red);
                }
                let mut txt = Text::from(format!("Network address: {}", cluster.addresses[id]));
                txt.extend(Text::styled(format!("{:#?}", report), report_style));
                let widget = Paragraph::new(txt)
                    .block(Block::default()
                        .borders(Borders::ALL)
                        .title(id.to_string()))
                    .wrap(tui::widgets::Wrap { trim: true });
                f.render_widget(widget, chunks[ix]);
            }
        }).unwrap();

        //std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

