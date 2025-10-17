import sys

def add_venv_libs_to_syspath(site_packages, with_pth=False):
    """
    Adds the site-packages folder (and .pth entries) from a virtual environment to sys.path.
    
    Args:
        venv_path (str): Path to the root of the virtual environment.
    """

    # Add site-packages itself
    if site_packages not in sys.path:
        sys.path.insert(0, site_packages)

    # Process .pth files inside site-packages
    if with_pth:
        import os
        for entry in os.listdir(site_packages):
            if entry.endswith(".pth"):
                pth_file = os.path.join(site_packages, entry)
                try:
                    with open(pth_file, "r") as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith("#"):
                                continue
                            if line.startswith("import "):
                                # Execute import statements inside .pth files
                                exec(line, globals(), locals())
                            else:
                                # Treat as a path
                                extra_path = os.path.join(site_packages, line)
                                if extra_path not in sys.path:
                                    sys.path.insert(0, extra_path)
                except Exception as e:
                    print(f"Warning: Could not process {pth_file}: {e}")

    return site_packages
