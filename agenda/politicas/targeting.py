from __future__ import annotations

import pandas as pd


def rule_bottom_tail(df: pd.DataFrame, q: float = 0.2) -> pd.Series:
    threshold = df["vulnerabilidad"].quantile(1 - q)
    return df["vulnerabilidad"] >= threshold


def rule_persistencia(df: pd.DataFrame, min_years: int = 8, q: float = 0.2) -> pd.Series:
    df = df.copy()
    df["is_bottom"] = rule_bottom_tail(df, q=q)
    counts = df.groupby("ubigeo")["is_bottom"].sum()
    return df["ubigeo"].map(counts >= min_years)


def rule_cluster_ll(df: pd.DataFrame, lisa_df: pd.DataFrame, indicator: str = "pib_pc") -> pd.Series:
    df = df.copy()
    key = df[["ubigeo", "anio"]]
    lisa = lisa_df[lisa_df["indicador"] == indicator]
    lisa = lisa[["ubigeo", "anio", "cluster"]]
    merged = key.merge(lisa, on=["ubigeo", "anio"], how="left")
    return merged["cluster"].fillna(0).astype(int).eq(3)


def rule_multiobjetivo(df: pd.DataFrame, pesos: dict[str, float]) -> pd.Series:
    score = 0.0
    if "vulnerabilidad" in df.columns:
        score += pesos.get("vulnerabilidad", 0.0) * df["vulnerabilidad"]
    if "dist_road" in df.columns:
        score += pesos.get("accesibilidad", 0.0) * (-df["dist_road"])
    if "mineria_expo" in df.columns:
        score += pesos.get("mineria", 0.0) * df["mineria_expo"]
    threshold = score.quantile(0.8)
    return score >= threshold


def seleccionar_beneficiarios(df: pd.DataFrame, regla: str, **kwargs) -> pd.Series:
    if regla == "bottom_tail":
        q = kwargs.get("q", 0.2)
        return rule_bottom_tail(df, q=q)
    if regla == "persistencia":
        q = kwargs.get("q", 0.2)
        min_years = kwargs.get("min_years", 8)
        return rule_persistencia(df, min_years=min_years, q=q)
    if regla == "cluster_ll":
        lisa_df = kwargs.get("lisa_df")
        indicator = kwargs.get("indicator", "pib_pc")
        return rule_cluster_ll(df, lisa_df, indicator=indicator)
    if regla == "multiobjetivo":
        pesos = kwargs.get("pesos", {})
        return rule_multiobjetivo(df, pesos)
    raise ValueError(f"unknown regla: {regla}")
