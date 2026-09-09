use std::path::PathBuf;
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};
use eframe::{Renderer, egui};

use crate::converter::{ConversionOptions, convert_source};

enum WorkerMessage {
    Log(String),
    Finished(Result<String, String>),
}

pub struct ConverterApp {
    source: String,
    destination: String,
    tick_size: String,
    logs: Vec<String>,
    converting: bool,
    sender: Sender<WorkerMessage>,
    receiver: Receiver<WorkerMessage>,
}

impl Default for ConverterApp {
    fn default() -> Self {
        let (sender, receiver) = unbounded();
        Self {
            source: String::new(),
            destination: String::new(),
            tick_size: String::new(),
            logs: Vec::new(),
            converting: false,
            sender,
            receiver,
        }
    }
}

impl ConverterApp {
    fn start_conversion(&mut self, ctx: egui::Context) {
        let source = PathBuf::from(self.source.trim());
        let destination = PathBuf::from(self.destination.trim());
        if self.source.trim().is_empty() || self.destination.trim().is_empty() {
            self.logs
                .push("ERROR: Source and Destination are required.".into());
            return;
        }
        let tick = self.tick_size.trim().to_string();
        let sender = self.sender.clone();
        self.converting = true;
        self.logs.clear();
        thread::spawn(move || {
            let log_sender = sender.clone();
            let repaint = ctx.clone();
            let logger = move |message: &str| {
                let _ = log_sender.send(WorkerMessage::Log(message.to_string()));
                repaint.request_repaint();
            };
            let result = convert_source(
                &source,
                &destination,
                ConversionOptions {
                    tick_size: if tick.is_empty() { None } else { Some(&tick) },
                    log: Some(&logger),
                },
            )
            .map(|result| {
                format!(
                    "Completed at {:.2} MB/s ({:.0} events/s)",
                    result.metrics.input_mb_per_second, result.metrics.events_per_second,
                )
            })
            .map_err(|error| format!("ERROR: {error:#}"));
            let _ = sender.send(WorkerMessage::Finished(result));
            ctx.request_repaint();
        });
    }

    fn receive_messages(&mut self) {
        while let Ok(message) = self.receiver.try_recv() {
            match message {
                WorkerMessage::Log(message) => self.logs.push(message),
                WorkerMessage::Finished(result) => {
                    self.converting = false;
                    match result {
                        Ok(message) => self.logs.push(message),
                        Err(error) => self.logs.push(error),
                    }
                }
            }
        }
    }
}

impl eframe::App for ConverterApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive_messages();
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("NRDToParquet");
            ui.add_space(10.0);
            egui::Grid::new("converter-fields")
                .num_columns(3)
                .spacing([8.0, 10.0])
                .show(ui, |ui| {
                    ui.label("Source:");
                    ui.add_sized([430.0, 24.0], egui::TextEdit::singleline(&mut self.source));
                    ui.menu_button("Browse", |ui| {
                        if ui.button("CSV File").clicked() {
                            if let Some(path) = rfd::FileDialog::new()
                                .add_filter("CSV", &["csv"])
                                .pick_file()
                            {
                                self.source = path.display().to_string();
                            }
                            ui.close();
                        }
                        if ui.button("Folder").clicked() {
                            if let Some(path) = rfd::FileDialog::new().pick_folder() {
                                self.source = path.display().to_string();
                            }
                            ui.close();
                        }
                    });
                    ui.end_row();

                    ui.label("Destination:");
                    ui.add_sized(
                        [430.0, 24.0],
                        egui::TextEdit::singleline(&mut self.destination),
                    );
                    if ui.button("Browse").clicked()
                        && let Some(path) = rfd::FileDialog::new().pick_folder()
                    {
                        self.destination = path.display().to_string();
                    }
                    ui.end_row();

                    ui.label("Tick Size:");
                    ui.add_sized(
                        [150.0, 24.0],
                        egui::TextEdit::singleline(&mut self.tick_size),
                    );
                    ui.label("");
                    ui.end_row();
                });
            ui.add_space(12.0);
            if ui
                .add_enabled(
                    !self.converting,
                    egui::Button::new("CONVERT").min_size([120.0, 32.0].into()),
                )
                .clicked()
            {
                self.start_conversion(ctx.clone());
            }
            ui.add_space(10.0);
            ui.separator();
            egui::ScrollArea::vertical()
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for line in &self.logs {
                        ui.monospace(line);
                    }
                });
        });
        if self.converting {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        }
    }
}

pub fn run() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([680.0, 480.0]),
        renderer: Renderer::Wgpu,
        ..Default::default()
    };
    eframe::run_native(
        "NRDToParquet",
        options,
        Box::new(|_context| Ok(Box::<ConverterApp>::default())),
    )
}
