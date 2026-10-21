//! CPU layout of a run: which CPUs the plugin, the driver, the decoder, and the fleet use.
//! `auto` reads the topology from /sys: the plugin gets whole cores on one L3 domain, the rest elsewhere.

use {
    crate::sys,
    anyhow::Context,
    serde::{Deserialize, Serialize},
    std::{collections::BTreeMap, fs, path::Path},
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub plugin: Vec<usize>,
    pub driver: Vec<usize>,
    pub decoder: Vec<usize>,
    pub probes: Vec<usize>,
    pub load: Vec<usize>,
    #[serde(default)]
    pub note: String,
}

impl Layout {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))
    }

    pub fn cpu_list(cpus: &[usize]) -> String {
        let mut out = Vec::new();
        let mut iter = cpus.iter().copied().peekable();
        while let Some(start) = iter.next() {
            let mut end = start;
            while iter.peek() == Some(&(end + 1)) {
                end = iter.next().expect("peeked");
            }
            out.push(if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            });
        }
        out.join(",")
    }
}

/// A physical core: its logical CPUs and the L3 domain it belongs to.
#[derive(Debug, Clone)]
struct Core {
    cpus: Vec<usize>,
    l3: String,
    l3_kib: u64,
}

/// Picks a layout for this machine, using only CPUs this process may run on.
pub fn auto(plugin_cores: usize) -> anyhow::Result<Layout> {
    let allowed = allowed_cpus()?;
    plan(read_cores(&allowed), allowed.len(), plugin_cores)
}

/// Physical cores among `allowed`, from /sys topology and cache files.
fn read_cores(allowed: &[usize]) -> Vec<Core> {
    let mut cores: BTreeMap<String, Core> = BTreeMap::new();
    for cpu in allowed {
        let base = format!("/sys/devices/system/cpu/cpu{cpu}");
        let siblings = read_trimmed(&format!("{base}/topology/thread_siblings_list"))
            .unwrap_or_else(|| cpu.to_string());
        let l3 = read_trimmed(&format!("{base}/cache/index3/shared_cpu_list"))
            .unwrap_or_else(|| "all".to_owned());
        let l3_kib = read_trimmed(&format!("{base}/cache/index3/size"))
            .and_then(|size| size.trim_end_matches('K').parse().ok())
            .unwrap_or(0);
        let core = cores.entry(siblings).or_insert_with(|| Core {
            cpus: Vec::new(),
            l3,
            l3_kib,
        });
        core.cpus.push(*cpu);
    }
    cores.into_values().collect()
}

/// Splits cores between roles: the plugin first, then one core each for probes, driver and decoder.
fn plan(mut cores: Vec<Core>, cpu_count: usize, plugin_cores: usize) -> anyhow::Result<Layout> {
    cores.sort_by_key(|core| core.cpus[0]);
    anyhow::ensure!(
        cores.len() >= 4,
        "need at least 4 cores, have {}",
        cores.len()
    );

    // The plugin takes the smallest L3 domain with enough cores, so it sees a production-sized cache.
    let mut domains: BTreeMap<String, (u64, Vec<usize>)> = BTreeMap::new();
    for (index, core) in cores.iter().enumerate() {
        domains
            .entry(core.l3.clone())
            .or_insert((core.l3_kib, Vec::new()))
            .1
            .push(index);
    }
    let want = plugin_cores.min(cores.len() / 2).max(1);
    let (domain, plugin_cores) = domains
        .iter()
        .filter(|(_, (_, members))| members.len() >= want)
        .min_by_key(|(_, (kib, members))| (*kib, std::cmp::Reverse(members.len())))
        .map(|(name, (_, members))| (name.clone(), members[members.len() - want..].to_vec()))
        .unwrap_or_else(|| {
            (
                "mixed".to_owned(),
                (cores.len() - want..cores.len()).collect(),
            )
        });
    let mut rest: Vec<usize> = (0..cores.len())
        .filter(|index| !plugin_cores.contains(index))
        .collect();
    // Other roles start outside the plugin's cache domain; its leftover cores go to the load fleet last.
    rest.sort_by_key(|index| (cores[*index].l3 == domain, *index));
    let mut take = |count: usize| -> Vec<usize> {
        let taken: Vec<usize> = rest
            .drain(..count.min(rest.len().saturating_sub(1)))
            .collect();
        taken
            .iter()
            .flat_map(|index| cores[*index].cpus.clone())
            .collect()
    };
    let probes = take(1);
    // Agave-shaped lanes run about seven driver threads; give them two cores when there is room.
    let mut driver = take(if cores.len() >= 12 { 2 } else { 1 });
    let mut decoder = take(1);
    let load: Vec<usize> = rest
        .iter()
        .flat_map(|index| cores[*index].cpus.clone())
        .collect();
    let plugin: Vec<usize> = plugin_cores
        .iter()
        .flat_map(|index| cores[*index].cpus.clone())
        .collect();
    // Too few cores for a core per role: the driver and decoder share with the probes.
    let shared = driver.is_empty() || decoder.is_empty();
    if driver.is_empty() {
        driver.clone_from(&probes);
    }
    if decoder.is_empty() {
        decoder.clone_from(&driver);
    }
    let mut layout = Layout {
        plugin,
        driver,
        decoder,
        probes,
        load,
        note: format!(
            "auto: {} cores ({cpu_count} CPUs) allowed; plugin on L3 domain {domain}{}",
            cores.len(),
            if shared {
                "; driver and decoder share the probe CPUs"
            } else {
                ""
            }
        ),
    };
    for list in [
        &mut layout.plugin,
        &mut layout.driver,
        &mut layout.decoder,
        &mut layout.probes,
        &mut layout.load,
    ] {
        list.sort_unstable();
    }
    Ok(layout)
}

fn allowed_cpus() -> anyhow::Result<Vec<usize>> {
    // SAFETY: cpu_set_t is plain data and sched_getaffinity writes inside it.
    let set = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        set
    };
    let online = read_trimmed("/sys/devices/system/cpu/online").unwrap_or_default();
    let online = sys::parse_cpu_list(&online).unwrap_or_default();
    // SAFETY: CPU_ISSET only reads inside the set.
    Ok(online
        .into_iter()
        .filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
        .collect())
}

fn read_trimmed(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|text| text.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::{plan, Core, Layout};

    /// `count` SMT cores; core `i` has CPUs `i` and `i + count`, and `l3` maps a core to its domain.
    fn cores(count: usize, l3: impl Fn(usize) -> (String, u64)) -> Vec<Core> {
        (0..count)
            .map(|i| {
                let (name, kib) = l3(i);
                Core {
                    cpus: vec![i, i + count],
                    l3: name,
                    l3_kib: kib,
                }
            })
            .collect()
    }

    fn assert_disjoint(layout: &Layout) {
        let mut all: Vec<usize> = [
            &layout.plugin,
            &layout.driver,
            &layout.decoder,
            &layout.probes,
            &layout.load,
        ]
        .iter()
        .flat_map(|list| list.iter().copied())
        .collect();
        let before = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(before, all.len(), "{layout:?}");
    }

    #[test]
    fn bundled_layouts_load() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("layouts");
        for entry in std::fs::read_dir(dir).unwrap() {
            let layout = Layout::load(&entry.unwrap().path()).unwrap();
            assert!(!layout.plugin.is_empty());
            assert_disjoint(&layout);
        }
    }

    #[test]
    fn cpu_list_round_trips() {
        assert_eq!(Layout::cpu_list(&[0, 1, 2, 5, 7, 8]), "0-2,5,7-8");
    }

    #[test]
    fn plugin_takes_the_smaller_cache_domain() {
        // 7950X3D: CCD0 (cores 0-7) has 96 MiB L3, CCD1 (cores 8-15) 32 MiB.
        let topology = cores(16, |i| {
            if i < 8 {
                ("ccd0".into(), 98_304)
            } else {
                ("ccd1".into(), 32_768)
            }
        });
        let layout = plan(topology, 32, 6).unwrap();
        assert_eq!(
            layout.plugin,
            vec![10, 11, 12, 13, 14, 15, 26, 27, 28, 29, 30, 31]
        );
        assert_eq!(layout.probes, vec![0, 16]);
        assert_eq!(layout.driver, vec![1, 2, 17, 18]);
        assert_eq!(layout.decoder, vec![3, 19]);
        assert_disjoint(&layout);
    }

    #[test]
    fn small_machines_still_get_every_role() {
        let layout = plan(cores(4, |_| ("all".into(), 0)), 8, 6).unwrap();
        assert_eq!(layout.plugin, vec![2, 3, 6, 7]);
        assert_eq!(layout.probes, vec![0, 4]);
        assert_eq!(layout.driver, layout.probes);
        assert_eq!(layout.decoder, layout.probes);
        assert_eq!(layout.load, vec![1, 5]);
        assert!(plan(cores(3, |_| ("all".into(), 0)), 6, 6).is_err());
    }
}
