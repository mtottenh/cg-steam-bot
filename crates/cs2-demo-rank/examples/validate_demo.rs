//! Cross-validation example: parse a demo with both our minimal parser
//! and the reference demoparser, then compare rank_update results.
//!
//! Also extracts per-player rank entity properties via the reference parser.

use ahash::AHashMap;
use parser::first_pass::parser_settings::ParserInputs;
use parser::parse_demo::{Parser, ParsingMode};
use parser::second_pass::parser_settings::create_huffman_lookup_table;
use parser::second_pass::variants::{VarVec, Variant};

/// Rank-related entity properties to extract per player.
const RANK_PROPS: &[(&str, &str)] = &[
    ("rank", "m_iCompetitiveRanking"),
    ("rank_if_win", "m_iCompetitiveRankingPredicted_Win"),
    ("rank_if_loss", "m_iCompetitiveRankingPredicted_Loss"),
    ("rank_if_tie", "m_iCompetitiveRankingPredicted_Tie"),
    ("comp_wins", "m_iCompetitiveWins"),
    ("comp_rank_type", "m_iCompetitiveRankType"),
];

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("cs2_demo_rank=debug")
        .init();

    let path = std::env::args()
        .nth(1)
        .expect("Usage: validate_demo <path.dem or path.dem.bz2>");

    println!("Reading {path}...");
    let demo_bytes = if path.ends_with(".bz2") {
        println!("Decompressing bzip2...");
        let decompressed =
            parallel_bzip2_decoder::decompress_file(&path).expect("bzip2 decompress failed");
        println!("Decompressed: {} bytes", decompressed.len());
        decompressed
    } else {
        let raw = std::fs::read(&path).expect("failed to read file");
        println!("Raw demo: {} bytes", raw.len());
        raw
    };

    // ---- Demo metadata ----
    println!("\n=== Demo metadata ===");
    match cs2_demo_rank::extract_demo_metadata(&demo_bytes) {
        Ok(meta) => println!("  map_name: {:?}", meta.map_name),
        Err(e) => eprintln!("  Failed to extract metadata: {e}"),
    }

    // ---- Our minimal parser ----
    println!("\n=== cs2-demo-rank (minimal parser) ===");
    let our_ranks =
        match cs2_demo_rank::extract_rank_updates(&demo_bytes) {
            Ok(ranks) => {
                println!("Found {} rank updates:", ranks.len());
                for r in &ranks {
                    println!(
                    "  steam_id64={} account_id={} rank_id={} rank_type={} wins={} change={:.2}",
                    r.steam_id64(), r.account_id, r.rank_id, r.rank_type_id, r.wins, r.rank_change
                );
                }
                ranks
            }
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        };

    // ---- Reference demoparser ----
    println!("\n=== Reference demoparser ===");
    let huf = create_huffman_lookup_table();
    let settings = ParserInputs {
        wanted_events: vec!["rank_update".to_string()],
        wanted_player_props: RANK_PROPS
            .iter()
            .map(|(_, prop)| prop.to_string())
            .collect(),
        wanted_other_props: vec![],
        wanted_players: vec![],
        wanted_ticks: vec![],
        real_name_to_og_name: AHashMap::default(),
        wanted_prop_states: AHashMap::default(),
        parse_ents: true,
        parse_projectiles: false,
        parse_grenades: false,
        only_header: false,
        only_convars: false,
        huffman_lookup_table: &huf,
        order_by_steamid: false,
        list_props: false,
        fallback_bytes: None,
    };

    let mut parser = Parser::new(settings, ParsingMode::ForceSingleThreaded);
    let output = match parser.parse_demo(&demo_bytes) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Reference parser error: {e:?}");
            std::process::exit(1);
        }
    };

    // -- rank_update game events --
    let rank_events: Vec<_> = output
        .game_events
        .iter()
        .filter(|e| e.name == "rank_update")
        .collect();

    println!("Found {} rank_update game events:", rank_events.len());
    for event in &rank_events {
        let get_i32 = |name: &str| -> Option<i32> {
            event
                .fields
                .iter()
                .find(|f| f.name == name)
                .and_then(|f| match &f.data {
                    Some(Variant::I32(v)) => Some(*v),
                    Some(Variant::U32(v)) => Some(*v as i32),
                    _ => None,
                })
        };
        let get_f32 = |name: &str| -> Option<f32> {
            event
                .fields
                .iter()
                .find(|f| f.name == name)
                .and_then(|f| match &f.data {
                    Some(Variant::F32(v)) => Some(*v),
                    _ => None,
                })
        };
        let get_str = |name: &str| -> Option<&str> {
            event
                .fields
                .iter()
                .find(|f| f.name == name)
                .and_then(|f| match &f.data {
                    Some(Variant::String(v)) => Some(v.as_str()),
                    _ => None,
                })
        };

        println!(
            "  steamid={:?} rank_old={:?} rank_new={:?} num_wins={:?} rank_type_id={:?} rank_change={:?}",
            get_str("user_steamid"),
            get_i32("rank_old"),
            get_i32("rank_new"),
            get_i32("num_wins"),
            get_i32("rank_type_id"),
            get_f32("rank_change"),
        );
    }

    // -- Per-player rank entity properties --
    println!("\n--- Per-player rank entity properties ---");
    let prop_controller = &output.prop_controller;

    // Build prop name -> id mapping for our requested props
    let prop_ids: Vec<(&str, Option<u32>)> = RANK_PROPS
        .iter()
        .map(|(label, prop_name)| {
            let id = prop_controller.name_to_id.get(*prop_name).copied();
            (*label, id)
        })
        .collect();

    for (steamid64, player_df) in &output.df_per_player {
        println!("  Player steamid64={steamid64}:");
        for (label, prop_id) in &prop_ids {
            let prop_id = match prop_id {
                Some(id) => *id,
                None => {
                    println!("    {label}: <prop not found>");
                    continue;
                }
            };
            let last_val = player_df.get(&prop_id).and_then(last_i32_value);
            match last_val {
                Some(v) => println!("    {label} = {v}"),
                None => println!("    {label} = <no data>"),
            }
        }
    }

    // ---- Compare ----
    println!("\n=== Comparison ===");
    println!(
        "Minimal parser: {} rank updates | Reference parser: {} rank_update events",
        our_ranks.len(),
        rank_events.len()
    );

    if our_ranks.len() == rank_events.len() {
        println!("Counts match.");
    } else {
        println!("WARNING: counts differ!");
    }
}

/// Get the last non-None i32 value from a PropColumn.
fn last_i32_value(col: &parser::second_pass::variants::PropColumn) -> Option<i32> {
    match &col.data {
        Some(VarVec::I32(vals)) => vals.iter().rev().find_map(|v| *v),
        Some(VarVec::U32(vals)) => vals.iter().rev().find_map(|v| v.map(|x| x as i32)),
        _ => None,
    }
}
