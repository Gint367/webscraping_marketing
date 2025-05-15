"""
Streamlit component for displaying and interacting with pipeline output artifacts.
"""

import io
import logging
import time
import zipfile
from pathlib import Path

import streamlit as st

from streamlit_app.app import JobDataModel

# Configure logging
logger = logging.getLogger(__name__)
# Ensure logger is configured (e.g., in main app or here if standalone)
if not logger.hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_file_type_display(file_path: Path) -> str:
    """Gets a display string for the file type."""
    if file_path.is_dir():
        return "Folder"
    if file_path.suffix.lower() == ".txt":
        return "text/plain"
    # Add more specific file types based on extension if needed
    return file_path.suffix.lstrip(".").upper() if file_path.suffix else "File"


def format_size(size_bytes: int) -> str:
    """Formats file size into a human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def zip_folder_in_memory(folder_path: Path) -> bytes:
    """Zips the contents of a folder into an in-memory bytes buffer."""
    logger.info(f"Zipping folder: {folder_path}")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path_item in folder_path.rglob("*"):
            zipf.write(file_path_item, file_path_item.relative_to(folder_path.parent))
    zip_buffer.seek(0)
    logger.info(
        f"Finished zipping {folder_path}. Size: {len(zip_buffer.getvalue())} bytes"
    )
    return zip_buffer.getvalue()


def clear_prepared_folder_download():
    """Clears any prepared folder download data from session state."""
    if "prepared_folder_download" in st.session_state:
        del st.session_state["prepared_folder_download"]
        logger.info("Cleared prepared folder download from session state.")


def _display_list_view_items(current_path: Path, base_output_dir: Path, job_id: str):
    """Helper function to display items in the list view."""
    items = []
    try:
        sorted_paths = sorted(
            list(current_path.iterdir()), key=lambda p: (not p.is_dir(), p.name.lower())
        )
        for item in sorted_paths:
            item_type = get_file_type_display(item)
            item_size = format_size(item.stat().st_size) if item.is_file() else "—"
            items.append(
                {
                    "Name": item.name,
                    "Type": item_type,
                    "Size": item_size,
                    "_path": item,
                }
            )
    except Exception as e:
        st.error(f"Error listing directory {current_path}: {e}")
        logger.error(f"Error listing directory {current_path}: {e}", exc_info=True)
        return

    if not items:
        st.info("This folder is empty.")
        if current_path != base_output_dir:
            if st.button(
                "⬆️ Back to Parent Folder",
                key=f"empty_folder_up_button_{current_path.as_posix()}",
            ):
                st.session_state[f"artifact_browse_path_{job_id}"] = str(
                    current_path.parent
                )
                st.rerun()
        return

    col_widths = (3, 1, 1, 1)
    header_cols = st.columns(col_widths, gap="small")
    header_cols[0].markdown("**Name**")
    header_cols[1].markdown("**Type**")
    header_cols[2].markdown("**Size**")
    header_cols[3].markdown("**Actions**")
    st.markdown("---")

    for item_data in items:
        item_path = item_data["_path"]
        cols = st.columns(col_widths, gap="small")

        with cols[0]:
            if item_path.is_dir():
                if st.button(
                    f"{item_data['Name']}",
                    key=f"nav_{item_path.as_posix()}",
                    use_container_width=False,
                    type="secondary",
                    icon="📁",
                ):
                    st.session_state[f"artifact_browse_path_{job_id}"] = str(item_path)
                    if "preview_file_path" in st.session_state:
                        del st.session_state.preview_file_path
                    if "preview_content" in st.session_state:
                        del st.session_state.preview_content
                    st.rerun()
            else:
                icon = "📄"
                if "text" in item_data["Type"].lower() or item_data["Type"] in [
                    "JSON",
                    "CSV",
                    "MD",
                    "PY",
                ]:
                    icon = "📝"
                elif item_data["Type"] in ["ZIP", "GZ", "TAR"]:
                    icon = "📦"
                st.markdown(f"{icon} {item_data['Name']}")

        cols[1].write(item_data["Type"])
        cols[2].write(item_data["Size"])

        with cols[3]:
            action_cols = st.columns([1, 1], vertical_alignment="center", gap="small")
            preview_button_placed = False

            if item_path.is_file() and item_path.suffix.lower() in [
                ".txt",
                ".log",
                ".md",
                ".json",
                ".csv",
                ".py",
                ".sh",
                ".html",
                ".xml",
            ]:
                if action_cols[0].button(
                    "",
                    key=f"preview_{item_path.as_posix()}",
                    help="Preview file content",
                    icon=":material/visibility:",
                ):
                    try:
                        content_preview = item_path.read_text(errors="ignore")[:2000]
                        st.session_state.preview_file_path = str(item_path)
                        st.session_state.preview_content = content_preview
                        st.rerun()
                    except Exception as e:
                        st.error(f"Preview error: {e}")
                        logger.error(
                            f"Error reading file {item_path} for preview: {e}",
                            exc_info=True,
                        )
                preview_button_placed = True

            target_col_idx = 1 if preview_button_placed and item_path.is_file() else 0
            target_col = action_cols[target_col_idx]

            if item_path.is_file():
                try:
                    target_col.download_button(
                        label="",
                        data=item_path.read_bytes(),
                        file_name=item_path.name,
                        mime="application/octet-stream",
                        key=f"download_file_{item_path.as_posix()}",
                        help="Download this file",
                        icon=":material/download:",
                    )
                except Exception as e:
                    target_col.error("DL Err")
                    logger.error(
                        f"Error preparing file {item_path} for download: {e}",
                        exc_info=True,
                    )
            elif item_path.is_dir():
                # Folder download logic
                """
                    - Initially displays a "Prepare folder for download" button (with a compress icon).
                    - When clicked:
                        - Clears any previously prepared folder download from the session state.
                        - Zips the selected folder in memory.
                        - Stores the zipped data, path, and filename in `st.session_state.prepared_folder_download`.
                        - Calls `st.rerun()` to refresh the UI.
                    - On the next page load, if the folder matches the one in `st.session_state.prepared_folder_download`:
                        - Displays a "Download folder as .zip" button (with a download icon).
                        - Clicking this button initiates the download and calls `clear_prepared_folder_download` to remove the zip data from the session state.
                    - If "Prepare folder for download" is clicked for a different folder, the process repeats and the previous zip (if any) is replaced.
                """
                prepared_info = st.session_state.get("prepared_folder_download", {})
                current_item_path_str = str(item_path)
                dir_action_col = action_cols[
                    0
                ]  # Directories typically use the first action column

                if prepared_info.get("path") == current_item_path_str:
                    # This folder's zip is ready for download
                    try:
                        dir_action_col.download_button(
                            label="",
                            data=prepared_info["data"],
                            file_name=prepared_info["name"],
                            mime="application/zip",
                            key=f"execute_download_folder_{item_path.as_posix()}",
                            help="Download folder as .zip",
                            icon=":material/download:",
                            on_click=clear_prepared_folder_download,  # Clear state after download click
                        )
                    except Exception as e:
                        st.error(
                            f"Error providing prepared zip for {item_path} for download: {e}"
                        )
                        logger.error(
                            f"Error providing prepared zip for {item_path} for download: {e}",
                            exc_info=True,
                        )
                        clear_prepared_folder_download()  # Clear potentially corrupt state
                else:
                    # Offer to prepare the download
                    if dir_action_col.button(
                        label="",
                        key=f"prepare_download_folder_{item_path.as_posix()}",
                        help="Prepare folder for download",
                        icon=":material/compress:",
                    ):
                        try:
                            # Clear any previously prepared folder first
                            clear_prepared_folder_download()

                            logger.info(f"Preparing zip for folder: {item_path}")
                            zip_data = zip_folder_in_memory(item_path)
                            st.session_state["prepared_folder_download"] = {
                                "path": current_item_path_str,
                                "data": zip_data,
                                "name": f"{item_path.name}.zip",
                            }
                            logger.info(f"Finished preparing zip for {item_path}")
                            st.rerun()  # Rerun to show the actual download button
                        except Exception as e:
                            st.error(f"Error zipping folder {item_path}: {e}")
                            logger.error(
                                f"Error zipping folder {item_path} for download: {e}",
                                exc_info=True,
                            )
                            clear_prepared_folder_download()  # Clean up session state
                            # Consider st.rerun() if error display needs to refresh other elements

    if "preview_file_path" in st.session_state and st.session_state.preview_file_path:
        preview_path = Path(st.session_state.preview_file_path)
        with st.expander(f"Preview: {preview_path.name}", expanded=True):
            if preview_path.suffix.lower() == ".csv":
                import pandas as pd

                try:
                    df = pd.read_csv(preview_path)
                    st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"Error reading CSV file: {e}")
                    logger.error(
                        f"Error reading CSV file {preview_path}: {e}", exc_info=True
                    )
            else:
                st.text_area(
                    "File Content (first 2000 characters):",
                    st.session_state.get("preview_content", ""),
                    height=300,
                    key="preview_text_area_content",
                    disabled=True,
                )
            if st.button("Close Preview", key="close_preview_button_expander"):
                del st.session_state.preview_file_path
                if "preview_content" in st.session_state:
                    del st.session_state.preview_content
                st.rerun()


def display_final_output_file_section(job_data: JobDataModel) -> None:
    """
    Displays a container with the final output file and a download button if it exists.

    Args:
        job_data (JobDataModel): The job data model loaded from the database.
    """
    output_final_file_path = getattr(job_data, "output_final_file_path", None)
    if output_final_file_path and Path(output_final_file_path).is_file():
        file_path = Path(output_final_file_path)
        with st.container():
            st.markdown("### Final Output File")
            st.success(
                f"**{file_path.name}** ({format_size(file_path.stat().st_size)})"
            )
            with open(file_path, "rb") as f:
                st.download_button(
                    label="Download Final Output",
                    data=f.read(),
                    file_name=file_path.name,
                    mime="application/octet-stream",
                    key=f"download_final_output_{file_path.name}",
                )
    else:
        st.info("No final output file available for this job.")


def display_output_section(conn):
    """Displays the pipeline artifacts browsing section."""
    st.subheader("Pipeline Artifacts")
    st.caption("Browse and download intermediate files generated during processing.")

    # --- Refactored for DB-driven job selection ---
    # Get all jobs from DB for selectbox
    from streamlit_app.utils import db_utils

    all_jobs_dict = db_utils.load_jobs_from_db(conn)
    job_ids = list(all_jobs_dict.keys())
    job_id_to_label = {}
    for job_id, job_data in all_jobs_dict.items():
        # Compose a label with file_info name and timestamp if available
        file_info = getattr(job_data, "file_info", {})
        file_info_name = file_info.get("name", f"Job {job_id}")
        start_time = getattr(job_data, "start_time", 0)
        timestamp = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(start_time if start_time else time.time()),
        )
        job_id_to_label[job_id] = f"{file_info_name} ({timestamp})"

    # Use selectbox to pick job, following streamlit_rules (value is job_id, label is formatted)
    selected_job_id = st.selectbox(
        "Select job to view artifacts:",
        options=job_ids,
        format_func=lambda job_id: job_id_to_label.get(job_id, job_id),
        key="selected_job_id",
    )

    # Load job data from DB if a job is selected
    job_data = None
    base_output_dir = None
    if selected_job_id:
        job_data = all_jobs_dict.get(selected_job_id)
        if job_data is not None:
            config = getattr(job_data, "config", {})
            base_output_dir_str = config.get("output_dir")
            if base_output_dir_str:
                base_output_dir = Path(base_output_dir_str)

    if (
        not selected_job_id
        or not job_data
        or not base_output_dir
        or not base_output_dir.exists()
        or not base_output_dir.is_dir()
    ):
        st.info(
            "No job selected or no output artifacts found. Please select a job to view its artifacts."
        )
        return

    # --- Final Output File Section ---
    display_final_output_file_section(job_data)

    view_mode_cols = st.columns([1, 1, 1, 2])
    if "artifact_view_mode" not in st.session_state:
        st.session_state.artifact_view_mode = "List View"
    current_view_mode = st.session_state.artifact_view_mode

    if view_mode_cols[0].button(
        "List View",
        use_container_width=True,
        type="primary" if current_view_mode == "List View" else "secondary",
        icon=":material/list:",
    ):
        st.session_state.artifact_view_mode = "List View"
        st.rerun()
    if view_mode_cols[1].button(
        "Tree View",
        use_container_width=True,
        type="primary" if current_view_mode == "Tree View" else "secondary",
        icon=":material/forest:",
    ):
        st.session_state.artifact_view_mode = "Tree View"
        st.rerun()
    if view_mode_cols[2].button(
        "Refresh",
        use_container_width=True,
        key="artifact_refresh_button_new",
        icon=":material/refresh:",
    ):
        if "preview_file_path" in st.session_state:
            del st.session_state.preview_file_path
        if "preview_content" in st.session_state:
            del st.session_state.preview_content
        st.rerun()

    browse_path_key = f"artifact_browse_path_{selected_job_id}"
    if (
        browse_path_key not in st.session_state
        or not Path(st.session_state[browse_path_key]).exists()
        or str(base_output_dir) not in str(Path(st.session_state[browse_path_key]))
    ):
        st.session_state[browse_path_key] = str(base_output_dir)
        if "preview_file_path" in st.session_state:
            del st.session_state.preview_file_path
        if "preview_content" in st.session_state:
            del st.session_state.preview_content

    current_path = Path(st.session_state[browse_path_key])

    phase_options = {"(Job Root)": base_output_dir}
    try:
        for item in base_output_dir.iterdir():
            if item.is_dir():
                phase_options[item.name] = item
    except Exception as e:
        logger.warning(f"Could not list phases in {base_output_dir}: {e}")

    # Determine what the selectbox *should* display based on the current_path
    # This will be the default selected key in the selectbox.
    current_phase_for_display = "(Job Root)"
    # Iterate to find which phase current_path belongs to, or if it's the root.
    # Sorting by length of path_val.name descending might be useful if phases could be nested
    # e.g. "phase1" and "phase1/sub", but for flat phase structures, order isn't critical.
    # However, to ensure the most specific parent phase is chosen if current_path is deep:
    # e.g. if phases are 'A' and 'A/B', and current_path is 'A/B/C', we want 'A/B' if it's an option.
    # For now, assuming phases are direct children of base_output_dir.
    for name, path_val in phase_options.items():
        if (
            name == "(Job Root)"
        ):  # Skip the root itself for relative_to checks initially
            continue
        if path_val == current_path:  # Exact match with a phase directory
            current_phase_for_display = name
            break
        try:
            # Check if current_path is a subdirectory of path_val
            current_path.relative_to(path_val)
            current_phase_for_display = name
            # Found the phase current_path belongs to.
            break
        except ValueError:
            # current_path is not relative to this path_val, or path_val is not an ancestor.
            continue
    # If no specific phase was matched and current_path is not base_output_dir, it implies it's under "(Job Root)"
    # but not directly one of the named phases (e.g. a file in Job Root, or deeper than named phases).
    # The initial default of "(Job Root)" handles cases where current_path is base_output_dir.

    # Get the value from the selectbox widget.
    # The index ensures it defaults to reflecting the current_phase_for_display.
    actual_selectbox_value_name = st.selectbox(
        "Quick Jump:",
        options=list(phase_options.keys()),
        index=list(phase_options.keys()).index(
            current_phase_for_display
        ),  # Default selection
        key=f"phase_jump_{selected_job_id}",
    )

    # Only act if the user *actively changed* the selectbox to a new phase.
    # This means the selectbox's current value (actual_selectbox_value_name)
    # is different from what we determined it *should be displaying* based on current_path (current_phase_for_display).
    if actual_selectbox_value_name != current_phase_for_display:
        new_path_from_selectbox = phase_options[actual_selectbox_value_name]
        # Ensure we are actually changing the path to avoid redundant reruns
        if new_path_from_selectbox != current_path:
            st.session_state[browse_path_key] = str(new_path_from_selectbox)
            if "preview_file_path" in st.session_state:
                del st.session_state.preview_file_path
            if "preview_content" in st.session_state:
                del st.session_state.preview_content
            st.rerun()

    path_parts = []
    temp_path = current_path
    while len(path_parts) < 10:  # Safety break for deep paths
        path_parts.append(temp_path)
        if temp_path == base_output_dir or temp_path.parent == temp_path:
            break  # Reached root or filesystem root
        temp_path = temp_path.parent
        if str(base_output_dir.parent) not in str(temp_path):
            break  # Moved outside job's parent scope
    path_parts.reverse()

    breadcrumb_str_parts = []
    for part_path in path_parts:
        name = "Root" if part_path == base_output_dir else part_path.name
        breadcrumb_str_parts.append(
            f"**{name}**" if part_path == current_path else name
        )
    st.markdown(f"Current Location: {' / '.join(breadcrumb_str_parts)}")

    if current_path != base_output_dir:
        parent_path = current_path.parent
        if (
            str(base_output_dir) in str(parent_path)
            or parent_path == base_output_dir
            or base_output_dir.parent == parent_path
        ):
            if st.button(
                "Up to Parent Folder",
                key=f"up_button_{current_path.as_posix()}",
                icon=":material/arrow_upward:",
            ):
                st.session_state[browse_path_key] = str(parent_path)
                if "preview_file_path" in st.session_state:
                    del st.session_state.preview_file_path
                if "preview_content" in st.session_state:
                    del st.session_state.preview_content
                st.rerun()
    st.markdown("---")

    if st.session_state.artifact_view_mode == "List View":
        _display_list_view_items(current_path, base_output_dir, selected_job_id)
    elif st.session_state.artifact_view_mode == "Tree View":
        st.info("Tree View is not yet implemented. Please use List View.")
    else:
        st.error("Invalid view mode selected.")
