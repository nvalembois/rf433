use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use futures_util::SinkExt;
use serde::Serialize;
use gpiod::{Chip, EdgeDetect, Options};

const GPIO_PIN:   u32 = 27;
const GAP_US:     Duration = Duration::from_micros(8000);
const MIN_US:     Duration = Duration::from_micros(5);
const MAX_PULSES: usize = 512;
const WS_PORT:    u16 = 8765;

type Pulse = i32;

#[derive(Clone, Serialize)]
struct Frame {
    ts:     f64,
    pulses: Vec<Pulse>,
}

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel::<Frame>(32);
    let tx      = Arc::new(tx);

    /* ── Thread GPIO (bloquant, hors tokio runtime) ── */
    let tx_gpio = tx.clone();
    std::thread::spawn(move || {
        if let Err(e) = gpio_capture(tx_gpio) {
            eprintln!("Erreur GPIO : {e}");
        }
    });

    /* ── Serveur WebSocket ── */
    let addr     = format!("0.0.0.0:{WS_PORT}");
    let listener = TcpListener::bind(&addr).await
        .expect("Impossible de binder le port WebSocket");
    println!("WebSocket en écoute sur {addr}");

    loop {
        let (stream, peer) = listener.accept().await.unwrap();
        println!("Client connecté : {peer}");

        let mut rx = tx.subscribe();
        tokio::spawn(async move {
            let mut ws = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => { eprintln!("Erreur WS handshake : {e}"); return; }
            };

            while let Ok(frame) = rx.recv().await {
                let json = serde_json::to_string(&frame).unwrap();
                if ws.send(json.into()).await.is_err() {
                    break;
                }
                println!("→ Trame envoyée : {} pulses", frame.pulses.len());
            }
            println!("Client déconnecté : {peer}");
        });
    }
}

fn now_f64() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn gpio_capture(tx: Arc<broadcast::Sender<Frame>>) -> Result<(), Box<dyn std::error::Error>> {
    let chip  = Chip::new("/dev/gpiochip0")?;

    let opts  = Options::input([GPIO_PIN])
        .edge(EdgeDetect::Both)   // détecter rising + falling
        .consumer("rf_2_ws");
    
    let mut current: Vec<Pulse> = Vec::with_capacity(MAX_PULSES);
    let mut current_pulse: Duration = Duration::default();  // nécessaire pour absorber les glitchs (front <5us)
    
    let mut inputs = chip.request_lines(opts)?;

    println!("GPIO {GPIO_PIN} en écoute...");

    // Lecture du premier front
    let event = inputs.read_event()?;
    let mut last_tick: Duration = event.time;
    let mut last_level: gpiod::Edge = event.edge;

    loop {
        /* wait_edge est bloquant — parfait pour un thread dédié */
        let event = inputs.read_event()?;

        let duration = last_tick.abs_diff(event.time);
        match last_tick.abs_diff(event.time) {
            duration if duration < MIN_US => continue,
            duration if ration < GAP_US =>
        }
        if duration < MIN_US {
            current_pulse += duration;
            continue;
        }

        let prev_level  = last_level;
        last_tick  = event.time;
        last_level = event.edge;

        if duration < GAP_US {


            /* Fin de trame */
            if current.len() > 10 {
                let frame = Frame {
                    ts:     now_f64(),
                    pulses: current.clone(),
                };
                let _ = tx.send(frame);
                println!("→ Trame capturée : {} pulses", current.len());
            }
            current.clear();
        } else if current.len() < MAX_PULSES {
            current.push(Pulse(label, duration_us as u32));
        }
    }
}
