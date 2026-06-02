use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use futures_util::SinkExt;
use serde::Serialize;
use gpiod::{Chip, EdgeDetect, Options};

const GPIO_PIN:u32 = 27;
const GAP:Duration = Duration::from_micros(5000);
const GLITCH:Duration = Duration::from_micros(5);
const MAX_PULSES: usize = 2048;
const MIN_PULSES: usize = 10;
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
    
    let mut inputs = chip.request_lines(opts)?;

    println!("GPIO {GPIO_PIN} en écoute...");

    // Attente du premier front
    print!("Attente du premier GAP");
    let event  = inputs.read_event()?;
    let mut last_tick: Duration = event.time;
    let mut last_level: gpiod::Edge;
    // Attente du premier GAP
    loop {
        let event = inputs.read_event()?;
        let duration = last_tick.abs_diff(event.time);
        print!(".");
        last_tick  = event.time;
        last_level = event.edge;
        if duration > GAP {
            println!(" {}", duration.as_micros());
            break;
        }
    }

    // main event loop
    loop {
        /* wait_edge est bloquant — parfait pour un thread dédié */
        let event = inputs.read_event()?;
        let duration = last_tick.abs_diff(event.time);
        
        // Glitch cumulatif
        if duration < GLITCH  && event.edge == last_level {
            continue;
        }
        last_tick  = event.time;
        last_level = event.edge;
        
        // Fin de trame
        if duration > GAP || duration < GLITCH {
            println!("GAP {}us, {}", duration.as_micros(), current.len());
            if current.len() > MIN_PULSES {
                let frame = Frame {
                    ts:     now_f64(),
                    pulses: current.clone(),
                };
                let _ = tx.send(frame);
                println!("→ Trame capturée : {} pulses", current.len());
                println!("→ Pulses : {:?}", &current[0..MIN_PULSES]);
            }
            current.clear();
            continue;
        }

        // Standard pulse
        if current.len() >= MAX_PULSES {
            print!(".");
            continue;
        }
        let pulse = match last_level {
            gpiod::Edge::Falling => duration.as_micros() as i32,
            gpiod::Edge::Rising => -(duration.as_micros() as i32),
        };
        current.push(pulse);
    }
}
