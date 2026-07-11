import re
import pandas as pd

def eic_pre_join(
    df_a: pd.DataFrame, 
    df_b: pd.DataFrame, 
    label_a: str, 
    label_b: str
) -> tuple[pd.DataFrame | None, pd.DataFrame, pd.DataFrame]:
    """Find guaranteed matches via shared EIC codes.
    
    If two records share a valid EIC (16 chars, 3rd char = 'W'),
    they are guaranteed to be the same plant. Returns:
      - links_df: DataFrame with matched indices
      - remaining_a: unmatched slice of df_a
      - remaining_b: unmatched slice of df_b
    """
    if "EIC" not in df_a.columns or "EIC" not in df_b.columns:
        return None, df_a, df_b

    eic_pattern = re.compile(r"^..W.{13}$")

    def get_valid_eics(series):
        result = {}
        for idx, val in series.items():
            try:
                if pd.isna(val):
                    continue
            except (ValueError, TypeError):
                pass
            if isinstance(val, set):
                for v in val:
                    if isinstance(v, str) and eic_pattern.match(v):
                        result.setdefault(v, []).append(idx)
            elif isinstance(val, str) and eic_pattern.match(val):
                result.setdefault(val, []).append(idx)
        return result

    eics_a = get_valid_eics(df_a["EIC"])
    eics_b = get_valid_eics(df_b["EIC"])

    shared = set(eics_a.keys()) & set(eics_b.keys())
    if not shared:
        return None, df_a, df_b

    eic_links = []
    matched_a, matched_b = set(), set()
    for eic in shared:
        for ia in eics_a[eic]:
            if ia in matched_a:
                continue
            for ib in eics_b[eic]:
                if ib in matched_b:
                    continue
                eic_links.append({label_a: ia, label_b: ib})
                matched_a.add(ia)
                matched_b.add(ib)
                break

    remaining_a = df_a.drop(index=list(matched_a), errors="ignore")
    remaining_b = df_b.drop(index=list(matched_b), errors="ignore")
    links_df = pd.DataFrame(eic_links, columns=[label_a, label_b])
    return links_df, remaining_a, remaining_b
