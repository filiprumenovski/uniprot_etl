use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use std::io::BufRead;

use crate::error::Result;
use crate::pipeline::handlers::{get_attribute, parse_evidence_refs, read_text, skip_element};
use crate::pipeline::scratch::{EntryScratch, FeatureContext};

pub fn consume_feature<R: BufRead>(
    reader: &mut Reader<R>,
    start: &BytesStart<'_>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    prepare_feature(start, scratch)?;
    let mut inner = Vec::new();

    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"location" => consume_location(reader, scratch, &mut inner)?,
                b"original" => {
                    let text = read_text(reader, b"original", &mut inner)?;
                    assign_original(scratch, text);
                }
                b"variation" => {
                    let text = read_text(reader, b"variation", &mut inner)?;
                    assign_variation(scratch, text);
                }
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner)?,
            },
            Event::Empty(e) => match e.local_name().as_ref() {
                b"location" => {}
                b"position" => handle_position_tag(&e, CoordinateType::Position, scratch)?,
                b"begin" => handle_position_tag(&e, CoordinateType::Begin, scratch)?,
                b"end" => handle_position_tag(&e, CoordinateType::End, scratch)?,
                _ => {}
            },
            Event::End(e) if e.local_name().as_ref() == b"feature" => {
                finalize_feature(scratch);
                break;
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(())
}

fn prepare_feature(start: &BytesStart<'_>, scratch: &mut EntryScratch) -> Result<()> {
    scratch.current_feature.clear();
    scratch.current_feature_context = FeatureContext::Generic;

    if let Some(id) = get_attribute(start, b"id")? {
        scratch.current_feature.id = Some(id);
    }
    if let Some(ft) = get_attribute(start, b"type")? {
        scratch.current_feature.feature_type = ft.clone();
        set_context(&ft, scratch);
    }
    if let Some(desc) = get_attribute(start, b"description")? {
        scratch.current_feature.description = Some(desc);
    }
    if let Some(ev) = get_attribute(start, b"evidence")? {
        scratch.current_feature.evidence_keys = parse_evidence_refs(&ev);
    }

    propagate_common_to_context(scratch);
    Ok(())
}

fn set_context(feature_type: &str, scratch: &mut EntryScratch) {
    scratch.current_feature_context = match feature_type {
        "active site" => FeatureContext::ActiveSite,
        "binding site" => FeatureContext::BindingSite,
        "metal ion-binding site" => FeatureContext::MetalCoordination,
        "mutagenesis site" => FeatureContext::Mutagenesis,
        "domain" => FeatureContext::Domain,
        "sequence variant" => FeatureContext::NaturalVariant,
        _ => FeatureContext::Generic,
    };

    match scratch.current_feature_context {
        FeatureContext::ActiveSite => scratch.current_active_site.clear(),
        FeatureContext::BindingSite => scratch.current_binding_site.clear(),
        FeatureContext::MetalCoordination => scratch.current_metal_coordination.clear(),
        FeatureContext::Mutagenesis => scratch.current_mutagenesis_site.clear(),
        FeatureContext::Domain => scratch.current_domain.clear(),
        FeatureContext::NaturalVariant => scratch.current_natural_variant.clear(),
        FeatureContext::Generic => {}
    }
}

fn propagate_common_to_context(scratch: &mut EntryScratch) {
    match scratch.current_feature_context {
        FeatureContext::ActiveSite => {
            scratch.current_active_site.id = scratch.current_feature.id.clone();
            scratch.current_active_site.description = scratch.current_feature.description.clone();
            scratch.current_active_site.evidence_keys = scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::BindingSite => {
            scratch.current_binding_site.id = scratch.current_feature.id.clone();
            scratch.current_binding_site.description = scratch.current_feature.description.clone();
            scratch.current_binding_site.evidence_keys = scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::MetalCoordination => {
            scratch.current_metal_coordination.id = scratch.current_feature.id.clone();
            scratch.current_metal_coordination.description = scratch.current_feature.description.clone();
            scratch.current_metal_coordination.evidence_keys =
                scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::Mutagenesis => {
            scratch.current_mutagenesis_site.id = scratch.current_feature.id.clone();
            scratch.current_mutagenesis_site.description = scratch.current_feature.description.clone();
            scratch.current_mutagenesis_site.evidence_keys =
                scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::Domain => {
            scratch.current_domain.id = scratch.current_feature.id.clone();
            scratch.current_domain.description = scratch.current_feature.description.clone();
            scratch.current_domain.evidence_keys = scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::NaturalVariant => {
            scratch.current_natural_variant.id = scratch.current_feature.id.clone();
            scratch.current_natural_variant.description = scratch.current_feature.description.clone();
            scratch.current_natural_variant.evidence_keys =
                scratch.current_feature.evidence_keys.clone();
        }
        FeatureContext::Generic => {}
    }
}

fn consume_location<R: BufRead>(
    reader: &mut Reader<R>,
    scratch: &mut EntryScratch,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut inner = Vec::new();
    loop {
        buf.clear();
        match reader.read_event_into(buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"position" => {
                    handle_position_tag(&e, CoordinateType::Position, scratch)?;
                    skip_element(reader, b"position", &mut inner)?;
                }
                b"begin" => {
                    handle_position_tag(&e, CoordinateType::Begin, scratch)?;
                    skip_element(reader, b"begin", &mut inner)?;
                }
                b"end" => {
                    handle_position_tag(&e, CoordinateType::End, scratch)?;
                    skip_element(reader, b"end", &mut inner)?;
                }
                _ => skip_element(reader, e.local_name().as_ref(), &mut inner)?,
            },
            Event::Empty(e) => match e.local_name().as_ref() {
                b"position" => handle_position_tag(&e, CoordinateType::Position, scratch)?,
                b"begin" => handle_position_tag(&e, CoordinateType::Begin, scratch)?,
                b"end" => handle_position_tag(&e, CoordinateType::End, scratch)?,
                _ => {}
            },
            Event::End(e) if e.local_name().as_ref() == b"location" => break,
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(())
}

fn handle_position_tag(
    e: &BytesStart<'_>,
    coord_type: CoordinateType,
    scratch: &mut EntryScratch,
) -> Result<()> {
    if let Some(pos) = get_attribute(e, b"position")? {
        if let Ok(p) = pos.parse() {
            apply_coordinate_to_feature(p, coord_type, scratch);
        }
    }
    Ok(())
}

fn assign_original(scratch: &mut EntryScratch, text: String) {
    match scratch.current_feature_context {
        FeatureContext::NaturalVariant => scratch.current_natural_variant.original = Some(text),
        _ => scratch.current_feature.original = Some(text),
    }
}

fn assign_variation(scratch: &mut EntryScratch, text: String) {
    match scratch.current_feature_context {
        FeatureContext::NaturalVariant => scratch.current_natural_variant.variation = Some(text),
        _ => scratch.current_feature.variation = Some(text),
    }
}

fn finalize_feature(scratch: &mut EntryScratch) {
    match scratch.current_feature_context {
        FeatureContext::ActiveSite => {
            scratch
                .entry
                .features
                .active_sites
                .push(std::mem::take(&mut scratch.current_active_site));
        }
        FeatureContext::BindingSite => {
            scratch
                .entry
                .features
                .binding_sites
                .push(std::mem::take(&mut scratch.current_binding_site));
        }
        FeatureContext::MetalCoordination => {
            scratch
                .entry
                .features
                .metal_coordinations
                .push(std::mem::take(&mut scratch.current_metal_coordination));
        }
        FeatureContext::Mutagenesis => {
            scratch
                .entry
                .features
                .mutagenesis_sites
                .push(std::mem::take(&mut scratch.current_mutagenesis_site));
        }
        FeatureContext::Domain => {
            scratch
                .entry
                .features
                .domains
                .push(std::mem::take(&mut scratch.current_domain));
        }
        FeatureContext::NaturalVariant => {
            scratch
                .entry
                .features
                .natural_variants
                .push(std::mem::take(&mut scratch.current_natural_variant));
        }
        FeatureContext::Generic => {}
    }

    scratch
        .entry
        .features
        .generic
        .push(std::mem::take(&mut scratch.current_feature));
    scratch.current_feature_context = FeatureContext::Generic;
}

#[derive(Debug, Clone, Copy)]
enum CoordinateType {
    Position,
    Begin,
    End,
}

/// Applies position coordinate to the appropriate feature buffer based on feature context
fn apply_coordinate_to_feature(pos: i32, coord_type: CoordinateType, scratch: &mut EntryScratch) {
    let apply_to_generic = |scratch: &mut EntryScratch| match coord_type {
        CoordinateType::Position => {
            scratch.current_feature.start = Some(pos);
            scratch.current_feature.end = Some(pos);
        }
        CoordinateType::Begin => {
            scratch.current_feature.start = Some(pos);
        }
        CoordinateType::End => {
            scratch.current_feature.end = Some(pos);
        }
    };

    match scratch.current_feature_context {
        FeatureContext::ActiveSite => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_active_site.start = Some(pos);
                    scratch.current_active_site.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_active_site.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_active_site.end = Some(pos);
                }
            }
        }
        FeatureContext::BindingSite => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_binding_site.start = Some(pos);
                    scratch.current_binding_site.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_binding_site.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_binding_site.end = Some(pos);
                }
            }
        }
        FeatureContext::MetalCoordination => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_metal_coordination.start = Some(pos);
                    scratch.current_metal_coordination.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_metal_coordination.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_metal_coordination.end = Some(pos);
                }
            }
        }
        FeatureContext::Mutagenesis => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_mutagenesis_site.start = Some(pos);
                    scratch.current_mutagenesis_site.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_mutagenesis_site.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_mutagenesis_site.end = Some(pos);
                }
            }
        }
        FeatureContext::Domain => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_domain.start = Some(pos);
                    scratch.current_domain.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_domain.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_domain.end = Some(pos);
                }
            }
        }
        FeatureContext::NaturalVariant => {
            apply_to_generic(scratch);
            match coord_type {
                CoordinateType::Position => {
                    scratch.current_natural_variant.start = Some(pos);
                    scratch.current_natural_variant.end = Some(pos);
                }
                CoordinateType::Begin => {
                    scratch.current_natural_variant.start = Some(pos);
                }
                CoordinateType::End => {
                    scratch.current_natural_variant.end = Some(pos);
                }
            }
        }
        FeatureContext::Generic => {
            apply_to_generic(scratch);
        }
    }
}
