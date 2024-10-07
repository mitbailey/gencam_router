//! 
//! This program routes communications between clients and producers.
//! 

use futures_util::{SinkExt, StreamExt};
use log::*;
use std::{net::SocketAddr, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Error, Message, Result},
};

use refimage::DynamicImageOwned;
use image::open;
use gencam_packet::*;

use futures_channel::mpsc::{unbounded, UnboundedSender};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<std::option::Option<gencam_packet::NodeID>, Tx>>>;

async fn load_and_transmit_debug_image(path: &str, i: i32) -> Vec<u8> {
    // Load the image as a DynamicImage.
    let img = open(path).expect("Could not load image");

    // This allows different image data to be sent using the same image. The hue is adjusted as per the passed i argument.
    let img = img.huerotate(90 * i);

    // Converts the DynamicImage to DynamicImageOwned.
    let img = DynamicImageOwned::try_from(img).expect("Could not convert image.");

    // Create a new GenCamPacket with the image data.
    let pkt = GenCamPacket::new(PacketType::Image, 0, 64, 64, Some(img.as_raw_u8().to_vec()));
    // Set msg to the serialized pkt.
    // let msg = serde_json::to_vec(&pkt).unwrap();
    serde_json::to_vec(&pkt).unwrap()
    // Send the message.
    // websocket.send(msg.into()).unwrap(); 
    // socket.write_all(&msg).await;
}

async fn accept_connection(peer: SocketAddr, stream: TcpStream, router_id: NodeID, peer_map: PeerMap) {
    if let Err(e) = handle_connection(peer, stream, router_id, peer_map).await {
        match e {
            Error::ConnectionClosed | Error::Protocol(_) | Error::Utf8 => (),
            err => error!("Error processing connection: {}", err),
        }
    }
}

async fn handle_connection(peer: SocketAddr, stream: TcpStream, router_id: NodeID, peer_map: PeerMap) -> Result<()> {
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    info!("New WebSocket connection: {}", peer);
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // A client has connected successfully at the TCP-level.
    // Next, we reply with an Ack containing information about what we (the router) shall be referred to as.
    // Construct and send this ID Reply.
    let frame_idr = GenCamFrame::identifier(router_id.clone(), NodeID { id: 0, name: "Any".to_owned() });
    let outbuf = serde_json::to_vec(&frame_idr).unwrap();
    ws_sender.send(Message::Binary(outbuf)).await?;

    // Sets the periodic interval.
    let mut interval = tokio::time::interval(Duration::from_millis(2500));
    
    let id: Option<NodeID>;

    // Stay here until we receive a NodeID from this connector.
    while id.is_none() {
        tokio::select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(msg) => {
                        let msg = msg?;
                        if msg.is_binary() { // Message is binary, lets look for our frame.
                            println!("Received binary message.");

                            let inbuf = msg.into_data();
                            let mut outbuf: Vec<u8> = Vec::new();

                            let frame: GenCamFrame = serde_json::from_slice(&inbuf).unwrap();

                            if frame.destination == router_id.clone() {
                                // Received a packet for the router itself.
                                match frame.frame_type {
                                    FrameType::Id => {
                                        // Received an ID packet type.
                                        println!("Received an ID frame type.");
                                        id = Some(frame.source.clone());
                                        // Reply with Ack
                                        let frame_ack = GenCamFrame::new(router_id.clone(), frame.source.clone(), PacketType::Ack, 0, 0, 0, None);
                                        break;
                                    },
                                    FrameType::Comms => {
                                        // Received a comms packet type.
                                        println!("Received a comms frame type but the sender has not yet identified itself!");
                                        println!("Resending the ID reply.");

                                        let frame_idr = GenCamFrame::identifier(router_id.clone(), NodeID { id: 0, name: "Any".to_owned() });
                                        let outbuf = serde_json::to_vec(&frame_idr).unwrap();
                                        ws_sender.send(Message::Binary(outbuf)).await?;
                                    },
                                }
                            }
                        } else if msg.is_close() {
                            println!("Received close message.");
                            break;
                        } else {
                            println!("Received an unhandled message type.");
                            warn!("Unexpected message type.");
                        }
                    }
                    None => break,
                }
            }
        }
    }

    // We have now established who we are talking to, so we can move on.

    // Here we link up the internal communications necessary to enable inter-connector communications.
    let (tx, mut rx) = unbounded(); // Create MPSC channel for this connector.
    peer_map.lock().unwrap().insert(id.clone(), tx); // Insert the sender into the peer map such that everyone has access.

    // Echo incoming WebSocket messages and send a message periodically every second.
    let mut i = 0;
    loop {
        println!("Looping...");

        // Handles external communications.
        tokio::select! {
            msg = ws_receiver.next() => {
                match msg {
                    Some(msg) => {
                        let msg = msg?;
                        if msg.is_binary() { // Message is binary, lets look for our packet.
                            println!("Received binary message.");

                            let inbuf = msg.into_data();
                            let frame: GenCamFrame = serde_json::from_slice(&inbuf).unwrap();

                            if frame.destination == router_id {
                                println!("Received a frame bound for the router. This is currently unhandled at this stage of comms. Dropping.");
                            }
                            else if frame.destination == id.clone().unwrap() {
                                // This should never happen, it makes no sense.
                                println!("Received a frame bound for our connector. This makes no sense and should not occur. Dropping.");
                            }
                            else {
                                println!("Received a frame for another connector which may or may not exist.");
    
                                // if frame.destination 
                                if let Some(peer) = peer_map.lock().unwrap().get(&Some(frame.destination)) {
                                    println!("Forwarding frame to connector.");
                                    let peer = peer.clone();
                                    peer.unbounded_send(Message::Binary(inbuf)).unwrap();
                                }
                                else
                                {
                                    println!("Received a frame for a non-existent connector. Dropping.");
                                }
                            }
                        } else if msg.is_close() {
                            println!("Received close message.");
                            break;
                        } else {
                            println!("Received an unhandled message type.");
                            warn!("Unexpected message type.");
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => { // Periodically sends this message (right now 1 Hz).
                println!("Transmitting an image packet.");
                i += 1;
                let outbuf = load_and_transmit_debug_image(&format!("res/test_{}.png", i%10), i).await;
                println!("TRANSMITTING {} BYTES.", outbuf.len());
                ws_sender.send(Message::Binary(outbuf)).await?;
            }
        }

        // Handles internal communications.
        tokio::select! {
            int_msg = rx.next() => {
                match int_msg {
                    Some(msg) => {
                        // let msg = msg?;
                        if msg.is_binary() { // Message is binary, lets look for our packet.
                            println!("Received binary message.");

                            let inbuf = msg.into_data();
                            let frame: GenCamFrame = serde_json::from_slice(&inbuf).unwrap();

                            if frame.destination == id.clone().unwrap() {
                                // Received an internal communique for our connector.
                                println!("Received an internal communique for our connector.");
                                // Forward it
                                ws_sender.send(Message::Binary(inbuf)).await?;
                            }
                            else
                            {
                                println!("Received an internal communique for another connector. Dropping.");
                            }
                        } else {
                            println!("Received an unhandled message type.");
                            warn!("Unexpected message type.");
                        }
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let addr = "127.0.0.1:9001";
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");
    info!("Listening on: {}", addr);

    let peer_map: PeerMap = Arc::new(Mutex::new(HashMap::new()));

    while let Ok((stream, _)) = listener.accept().await {
        let peer = stream.peer_addr().expect("connected streams should have a peer address");
        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(peer, stream, NodeID { id: 65535, name: "Router".to_owned() }, peer_map.clone()));
    }
}
