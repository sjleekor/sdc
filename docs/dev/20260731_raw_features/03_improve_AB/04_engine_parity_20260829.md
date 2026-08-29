# Horizon Scan legacy/native full parity

- 판정: **실패**
- tolerance: `1e-12`
- Phase A replicate: `100`
- Phase B joint replicate: `100`
- legacy run: `{'A': '20260829T092904-00dfc5aa', 'B': '20260829T125427-00dfc5aa', 'AB': '20260829T161238-00dfc5aa'}`
- native run: `{'A': '20260827T221729-4e0ae8b0', 'B': '20260828T123313-4e0ae8b0', 'AB': '20260828T165038-4e0ae8b0'}`

| artifact | rows | max scaled delta | 판정 |
|---|---:|---:|---|
| `phase_a_horizon_ic` | 412 | 1.630e-15 | 통과 |
| `phase_a_permutation_cells` | 7500 | 1.665e-15 | 통과 |
| `phase_b_horizon_ic` | 288 | 3.146e-02 | 실패 |
| `phase_b_event_ic` | 6 | 0.000e+00 | 통과 |
| `phase_b_permutation_summary` | 100 | 0.000e+00 | 실패 |
| `phase_ab_primary` | 153 | 1.337e-01 | 실패 |
| `phase_ab_overlay` | 75 | 4.441e-16 | 통과 |

## SUE sorted-v2

real SUE cell은 `6`개이며 모두 `insufficient`입니다. 따라서 이번 snapshot에서 SUE가 joint null에 더한 유효 row는 없습니다.

## AB manifest 판정 요약

- 판정: **통과**

| 항목 | legacy | native |
|---|---:|---:|
| `config_hash` | 889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea | 889c3e8377c2f400907611f7402651eee6a23c2765c051e4eb2a4a59ca36cbea |
| `m_ab` | 153 | 153 |
| `phase_b_screen_pass_count` | 40 | 40 |
| `phase_b_evidence_grade_counts` | {'A': 23, 'B': 17, 'C': 35, 'D': 3} | {'A': 23, 'B': 17, 'C': 35, 'D': 3} |
| `phase_b_primary_discovery_count` | 55 | 55 |
| `phase_a_primary_discovery_count` | 32 | 32 |
| `phase_a_discovery_change_count` | 0 | 0 |
| `combined_cross_sectional_permutation_empirical_p` | 0.009900990099009901 | 0.009900990099009901 |
