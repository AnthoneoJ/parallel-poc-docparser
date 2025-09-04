import os
import io
import tempfile
import re
import pandas as pd
import streamlit as st
import camelot
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from rapidfuzz import fuzz

APP_VERSION = "v0.0.1 (2025-09-03)"  # <- update this when you ship

st.set_page_config(page_title="PDF → Tables Cleaner", layout="wide")
st.title("PDF Table Merge & Cleanup (Camelot → Ag-Grid)")

# ---------------- Helpers ----------------
def parse_excel_to_all_dfs(
    file_bytes: bytes,
    sheet: str | int | None,
    first_row_is_header: bool,
    skiprows: int = 0,
    skipcols: int = 0,
    last_row: int = 0,   # 1-based, 0 = till end
    last_col: int = 0,   # 1-based, 0 = till end
):
    """
    Return list[pd.DataFrame] from an Excel file, cropped to a rectangle.
    Cropping logic:
      - Drop the first `skiprows` rows and first `skipcols` columns
      - If last_row > 0, keep rows up to `last_row` (1-based) AFTER the initial sheet start
      - If last_col > 0, keep cols up to `last_col` (1-based) AFTER the initial sheet start
    """
    header = 0 if first_row_is_header else None
    all_dfs = []

    bio = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)
    sheet_names = xls.sheet_names
    targets = sheet_names if sheet is None else [sheet]

    for s in targets:
        # Read after skipping top rows
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=s, header=header, dtype=str, skiprows=skiprows)

        # Apply last_row (relative to the sheet start; after skipping)
        if last_row and last_row > 0:
            # Convert to 0-based slice length AFTER skiprows
            nrows_after_skip = max(last_row - skiprows, 0)
            df = df.iloc[:nrows_after_skip, :]

        # Cut left columns, then apply last_col relative to that
        df = df.iloc[:, skipcols:]
        if last_col and last_col > 0:
            ncols_after_skip = max(last_col - skipcols, 0)
            df = df.iloc[:, :ncols_after_skip]

        df.columns = [str(c) for c in df.columns]
        all_dfs.append(df)

    return all_dfs, sheet_names

def parse_csv_to_all_dfs(
    file_bytes: bytes,
    first_row_is_header: bool,
    sep: str = ",",
    skiprows: int = 0,
    skipcols: int = 0,
    last_row: int = 0,   # 1-based, 0 = till end
    last_col: int = 0,   # 1-based, 0 = till end
):
    """
    Return list[pd.DataFrame] from a CSV file (single table).
    """
    header = 0 if first_row_is_header else None

    # Read after skipping top rows
    df = pd.read_csv(io.BytesIO(file_bytes), header=header, sep=sep, dtype=str, encoding="utf-8-sig", skiprows=skiprows)

    # Apply last_row (after skips)
    if last_row and last_row > 0:
        nrows_after_skip = max(last_row - skiprows, 0)
        df = df.iloc[:nrows_after_skip, :]

    # Columns window
    df = df.iloc[:, skipcols:]
    if last_col and last_col > 0:
        ncols_after_skip = max(last_col - skipcols, 0)
        df = df.iloc[:, :ncols_after_skip]

    df.columns = [str(c) for c in df.columns]
    return [df]

def prepend_header_as_row(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c) for c in df.columns]
    header_row = pd.DataFrame([cols], columns=cols)
    return pd.concat([header_row, df.reset_index(drop=True)], ignore_index=True)

def normalize_and_concat(dfs, fill_value=""):
    """Prepend header rows, pad to the widest table, rename columns to col_1.., then concat."""
    if not dfs:
        return pd.DataFrame()

    dfs = [prepend_header_as_row(df) for df in dfs]
    max_cols = max(df.shape[1] for df in dfs)

    norm = []
    for df in dfs:
        df2 = df.copy()
        df2.columns = [str(c) for c in df2.columns]
        # pad or trim
        if df2.shape[1] < max_cols:
            for k in range(df2.shape[1], max_cols):
                df2[f"__pad_{k+1}"] = fill_value
        elif df2.shape[1] > max_cols:
            df2 = df2.iloc[:, :max_cols]
        df2.columns = [f"col_{i+1}" for i in range(max_cols)]
        norm.append(df2.reset_index(drop=True))

    out = pd.concat(norm, ignore_index=True)
    # add stable row ids for deletion
    out["_rid"] = range(len(out))
    return out

def apply_header_row(df: pd.DataFrame, header_idx: int, ensure_unique: bool = False):
    """
    Promote the row at header_idx to be the header *as-is* (no lowercasing, no regex).
    Returns (df_with_header, header_vals).

    If ensure_unique=True, only then suffix duplicates with _2, _3, ...;
    otherwise duplicate column names are allowed (pandas can handle them, but be careful).
    """
    # Preserve internal id if present
    has_rid = "_rid" in df.columns
    body_cols = [c for c in df.columns if c != "_rid"]

    # Get raw header values exactly as the user sees them
    header_vals = df.loc[header_idx, body_cols].tolist()

    # Optionally enforce unique column names without altering originals unless needed
    if ensure_unique:
        seen = {}
        uniq = []
        for h in header_vals:
            h = "" if h is None else str(h)
            if h in seen:
                seen[h] += 1
                uniq.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 1
                uniq.append(h)
        header_out = uniq
    else:
        header_out = header_vals

    # Drop the header row from the data (don’t transform any cell values)
    df2 = df.drop(index=header_idx).reset_index(drop=True)

    # Reorder columns so body columns are first, then _rid (if present)
    ordered_cols = body_cols + (["_rid"] if has_rid else [])
    df2 = df2[ordered_cols]

    # Set columns exactly as chosen header row (plus _rid if present)
    df2.columns = header_out + (["_rid"] if has_rid else [])

    return df2, header_vals  # header_vals are the raw originals

def is_header_like(row_vals, header_vals, min_ratio=90):
    sims = []
    for a, b in zip(row_vals, header_vals):
        a, b = str(a or "").strip(), str(b or "").strip()
        sims.append(fuzz.token_set_ratio(a, b) if (a or b) else 100)
    return (sum(sims) / max(len(sims), 1)) >= min_ratio

def drop_header_like_rows(df: pd.DataFrame, header_vals, min_ratio=90):
    body_cols = [c for c in df.columns if c != "_rid"]
    keep = []
    for _, row in df.iterrows():
        if not is_header_like([row[c] for c in body_cols], header_vals, min_ratio):
            keep.append(True)
        else:
            keep.append(False)
    out = df.loc[keep].reset_index(drop=True)
    out["_rid"] = range(len(out))
    return out

def parse_pdf_to_all_dfs(pdf_bytes: bytes):
    """Parse a PDF bytes object with Camelot, return list[pd.DataFrame]."""
    # write to temp file for Camelot
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    all_dfs = []

    try:
        # 1) Try lattice (works best with ruled tables)
        tables = camelot.read_pdf(tmp_path, pages="all", flavor="lattice")
        if len(tables) == 0:
            # 2) fallback to stream (works for borderless tables)
            tables = camelot.read_pdf(tmp_path, pages="all", flavor="stream")

        for t in tables:
            df = t.df
            # Your logic: promote first row to header, drop that row
            if df.shape[0] > 0:
                df.columns = df.iloc[0]
                df = df.drop(0).reset_index(drop=True)
            # Standardize column names to strings
            df.columns = [str(c) for c in df.columns]
            all_dfs.append(df)

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    return all_dfs

# ---------------- Sidebar: Upload & Parse ----------------
with st.sidebar:
    st.divider()
    st.caption(f"App {APP_VERSION}")

    st.header("1) Upload file")
    upl = st.file_uploader("Choose a PDF / Excel / CSV", type=["pdf", "xlsx", "xls", "csv"])

    filetype = None
    if upl is not None:
        name = upl.name.lower()
        if name.endswith(".pdf"):
            filetype = "pdf"
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            filetype = "excel"
        elif name.endswith(".csv"):
            filetype = "csv"

    # Common options for tabular files (Excel/CSV)
    first_row_is_header = st.checkbox("First row contains headers", value=True, help="For Excel/CSV only")

    # Excel/CSV region cropping
    skiprows = st.number_input("Skip N rows (top)", min_value=0, value=0, step=1)
    skipcols = st.number_input("Skip N columns (left)", min_value=0, value=0, step=1)
    last_row = st.number_input("Last data row (1-based, 0 = until end)", min_value=0, value=0, step=1)
    last_col = st.number_input("Last data column (1-based, 0 = until end)", min_value=0, value=0, step=1)

    # Excel sheet selection UI (shown only when an Excel is uploaded)
    selected_sheet = None
    parse_all_sheets = False
    excel_sheet_names = []
    if filetype == "excel" and upl is not None:
        # Peek the workbook to list sheets
        _, excel_sheet_names = parse_excel_to_all_dfs(upl.read(), sheet=None, first_row_is_header=first_row_is_header)
        # re-read as the previous call consumed the stream
        upl.seek(0)

        if len(excel_sheet_names) > 1:
            mode = st.radio("Sheet mode", ["Select one sheet", "Parse all sheets"], index=0, horizontal=True)
            if mode == "Parse all sheets":
                parse_all_sheets = True
            else:
                selected_sheet = st.selectbox("Select sheet", excel_sheet_names, index=0)
        else:
            st.caption(f"Sheet: {excel_sheet_names[0]}")
            selected_sheet = excel_sheet_names[0]

    run_parse = st.button("Parse file")

if "concat_df" not in st.session_state:
    st.session_state.concat_df = pd.DataFrame()

if run_parse:
    if not upl:
        st.warning("Please upload a file first.")
    else:
        file_bytes = upl.read()
        all_dfs = []

        if filetype == "pdf":
            # existing PDF → Camelot code path you already have
            all_dfs = parse_pdf_to_all_dfs(file_bytes)

        elif filetype == "excel":
            if parse_all_sheets:
                all_dfs, _ = parse_excel_to_all_dfs(
                    file_bytes,
                    sheet=None,
                    first_row_is_header=first_row_is_header,
                    skiprows=skiprows,
                    skipcols=skipcols,
                    last_row=last_row,
                    last_col=last_col,
                )
            else:
                # If workbook has only one sheet, selected_sheet is set above
                all_dfs, _ = parse_excel_to_all_dfs(
                    file_bytes,
                    sheet=selected_sheet,
                    first_row_is_header=first_row_is_header,
                    skiprows=skiprows,
                    skipcols=skipcols,
                    last_row=last_row,
                    last_col=last_col,
                )

        elif filetype == "csv":
            all_dfs = parse_csv_to_all_dfs(
                file_bytes,
                first_row_is_header=first_row_is_header,
                sep=",",
                skiprows=skiprows,
                skipcols=skipcols,
                last_row=last_row,
                last_col=last_col,
            )

        else:
            st.error("Unsupported file type.")
            all_dfs = []

        if not all_dfs:
            st.error("No tables detected or file is empty.")
        else:
            st.success(f"Parsed {len(all_dfs)} table(s).")
            concat_df = normalize_and_concat(all_dfs)  # uses your existing function
            st.session_state.concat_df = concat_df

# ---------------- 2) Editable Grid ----------------
st.subheader("2) Edit merged rows (Ag-Grid)")
if st.session_state.concat_df.empty:
    st.info("Upload and parse a PDF to begin. The merged grid will appear here.")
else:
    # Work on a copy so we can add a delete flag without mutating the original yet
    df = st.session_state.concat_df.copy()

    # ➊ Ensure a boolean "delete" column exists (users tick this to mark rows for removal)
    if "delete" not in df.columns:
        df["delete"] = False

    # ➋ Build grid (no row selection needed)
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=True, resizable=True)
    gb.configure_column("_rid", hide=True)
    gb.configure_column("delete", header_name="🗑 Delete?", editable=True)
    grid_options = gb.build()

    # ➌ Render editable grid and capture edits
    grid_resp = AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,   # edits flow back on change
        fit_columns_on_grid_load=True,
        height=420,
        enable_enterprise_modules=False,
    )

    edited_df = pd.DataFrame(grid_resp["data"])
    # Persist all edits (including delete ticks) to session state
    st.session_state.concat_df = edited_df

    # ➍ Delete rows that are checked
    colA, colB = st.columns([1, 1])

    with colA:
        to_delete = edited_df.loc[edited_df.get("delete", False) == True, "_rid"].tolist()
        st.caption(f"Checked for deletion: {len(to_delete)} row(s)")
        if st.button("Delete checked rows", type="primary", disabled=(len(to_delete) == 0)):
            kept = edited_df[~edited_df["_rid"].isin(to_delete)].drop(columns=["delete"], errors="ignore").reset_index(drop=True)
            kept["_rid"] = range(len(kept))
            st.session_state.concat_df = kept
            st.success(f"Deleted {len(to_delete)} row(s).")

    with colB:
        # 🔄 Refresh button — forces a rerun
        if st.button("🔄 Refresh table"):
            try:
                st.rerun()                   # Streamlit ≥1.30
            except Exception:
                st.experimental_rerun()      # fallback for older versions


# ---------------- 3) Pick Header + Clean ----------------
st.subheader("3) Pick header row & remove header-like duplicates")
if st.session_state.concat_df.empty:
    st.info("Header tools will show after parsing a PDF.")
else:
    df = st.session_state.concat_df
    header_idx = st.number_input("Header row index (0-based)", min_value=0, max_value=len(df)-1, value=0, step=1)
    if st.button("Apply header"):
        df_with_header, header_vals = apply_header_row(df, int(header_idx))
        st.success("Header applied.")
        st.dataframe(df_with_header.head(15), use_container_width=True)

        st.write("Remove rows similar to header:")
        min_ratio = st.slider("Similarity threshold", 70, 100, 90, 1)
        cleaned = drop_header_like_rows(df_with_header, header_vals, min_ratio=min_ratio)
        st.caption(f"Rows after cleaning: {len(cleaned)}")
        st.dataframe(cleaned.head(60), use_container_width=True)
        print(cleaned)

        st.download_button(
            "Download cleaned CSV",
            data=cleaned.drop(columns=["_rid"]).to_csv(index=False, encoding="utf-8-sig"),
            file_name="cleaned_tables.csv",
            mime="text/csv",
        )

st.caption("Tip: Camelot works best on digital PDFs. For scanned PDFs, consider OCR then table detection.")
