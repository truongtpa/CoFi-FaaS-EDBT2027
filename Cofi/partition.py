from typing import Dict, List

def rg_size_mb(rg: Dict, columns: List[str] | None) -> float:
    if columns:
        col_map = {c["name"]: float(c.get("compressed_size_mb") or 0.0) for c in rg.get("columns") or []}
        matched = [col_map[c] for c in columns if c in col_map]
        if matched:
            return sum(matched)
    return float(rg.get("total_byte_size_mb") or 0.0)


def partition_tasks(files: List[Dict], max_size_mb: float = 50, split_row_groups: bool = False,
                    columns: List[str] | None = None, single_file: bool = False) -> List[List[Dict]]:
    stream = []
    for f in files:
        file_size = float(f.get("size_mb") or 0.0)
        rgs = f.get("row_groups") or []
        if split_row_groups and rgs:
            rg_sizes = [rg_size_mb(rg, columns) for rg in rgs]
            rg_size_sum = sum(rg_sizes)
            use_file_size = rg_size_sum < 1.0 and file_size > rg_size_sum
            per_rg_size = (file_size / len(rgs)) if use_file_size else None
            for rg, rg_size in zip(rgs, rg_sizes):
                stream.append({"full_path": f["full_path"], "rg_id": rg["id"],
                               "size_mb": per_rg_size if use_file_size else rg_size})
        else:
            stream.append({"full_path": f["full_path"], "rg_id": None, "size_mb": file_size})
    if single_file:
        return [[item] for item in stream]
    groups, bucket, bucket_size = [], [], 0.0
    for item in stream:
        if bucket_size + item["size_mb"] > max_size_mb and bucket:
            groups.append(bucket)
            bucket, bucket_size = [], 0.0
        bucket.append(item)
        bucket_size += item["size_mb"]
        if bucket_size >= max_size_mb:
            groups.append(bucket)
            bucket, bucket_size = [], 0.0
    if bucket:
        groups.append(bucket)
    return groups
