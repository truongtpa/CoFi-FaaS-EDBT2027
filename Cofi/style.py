import os
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FONT_SIZE = 13
DPI = 300
INK, INK2, GRID = "#000000", "#52514e", "#e4e3df"
COST_UNIT, COST_SCALE = "¢", 100


def apply(font_size=FONT_SIZE):
    fm.fontManager.addfont(os.path.join(ROOT, "fonts", "LinLibertine_R.otf"))
    plt.rcParams.update({
        "font.family": "Linux Libertine O", "font.serif": ["Linux Libertine O"], "font.size": font_size,
        "axes.linewidth": 0.8, "grid.linewidth": 0.5, "lines.linewidth": 0.8,
        "axes.grid": True, "legend.frameon": False, "pdf.fonttype": 42, "ps.fonttype": 42,
        "savefig.dpi": DPI, "savefig.bbox": "tight",
        "mathtext.fontset": "custom", "mathtext.rm": "Linux Libertine O", "mathtext.it": "Linux Libertine O",
        "mathtext.bf": "Linux Libertine O", "axes.unicode_minus": True,
        "axes.edgecolor": INK2, "grid.color": GRID, "text.color": "black", "axes.labelcolor": "black",
        "axes.titlecolor": "black", "xtick.color": INK2, "ytick.color": INK2, "xtick.labelcolor": "black",
        "ytick.labelcolor": "black", "legend.labelcolor": "black",
    })


def save(fig, out_dir, name):
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), dpi=DPI, bbox_inches="tight", pad_inches=0.02)
