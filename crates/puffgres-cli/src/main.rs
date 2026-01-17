use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "puffgres")]
#[command(about = "Mirror Postgres data to turbopuffer")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new puffgres project
    Init,
    /// Start the CDC replication loop
    Run,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => println!("puffgres init - not yet implemented"),
        Commands::Run => println!("puffgres run - not yet implemented"),
    }
}
