![ConfigX Banner](assets/banners/banner_configx_contribs.png?raw=true)

# Contributing to ConfigX

First off, welcome! We are thrilled that you are interested in contributing to **ConfigX**.

Whether you are a participant in **GDSC MUJ Winter of Code**, a seasoned open-source developer, or a first-time contributor, we value your input. This document is designed to guide you through the process of setting up your development environment and submitting contributions.

---

## [ Table of Contents ]

1.  [Code of Conduct](#code-of-conduct)
2.  [Development Setup](#development-setup)
3.  [Coding Guidelines](#coding-guidelines)
4.  [Contribution Workflow](#contribution-workflow)
5.  [Reporting Bugs & Suggestions](#reporting-bugs--suggestions)
6.  [Getting Help](#getting-help)

---

## [ Code of Conduct ]

We expect all contributors to adhere to our Code of Conduct to ensure a welcoming and inclusive environment.

*   **[+] Be Respectful:** Treat everyone with respect, kindness, and empathy.
*   **[+] Be Professional:** Keep feedback constructive and focused on the code/project.
*   **[+] Be Inclusive:** We welcome contributors from all backgrounds and skill levels.
*   **[!] Zero Tolerance:** Harassment, hate speech, or abusive behavior will not be tolerated and may result in a ban.

---

## [ Development Setup ]

Follow these steps to set up ConfigX locally for development.

### 1. Fork and Clone
Fork the repository to your GitHub account, then clone it locally:

```bash
git clone https://github.com/GDSC-Manipal-University-Jaipur/ProjectConfigX.git
cd ProjectConfigX
```

### 2. Set Up Virtual Environment
It is recommended to use a virtual environment to keep dependencies isolated.

| Platform | Command |
| :--- | :--- |
| **Windows** | `python -m venv venv`<br>`.\venv\Scripts\activate` |
| **macOS/Linux** | `python3 -m venv venv`<br>`source venv/bin/activate` |

### 3. Install Dependencies
Install the project in **editable mode** to see changes instantly.

```bash
pip install --upgrade pip
pip install -e .
pip install pytest
pip install pytest-shutil
```

### 4. Run Tests
Ensure everything is working correctly by running the test suite.

```bash
pytest tests/
```

---

## [ Coding Guidelines ]

To maintain a high-quality codebase, please adhere to the following guidelines.

### Style & Naming Standards

| Category | Standard | Example |
| :--- | :--- | :--- |
| **Python Style** | PEP 8 | Standard Python indentation and spacing. |
| **Variables** | `snake_case` | `user_id`, `calculate_total` |
| **Classes** | `CamelCase` | `ConfigTree`, `StorageRuntime` |
| **Constants** | `UPPER_CASE` | `MAX_RETRIES`, `DEFAULT_PATH` |
| **Internal** | Prefix with `_` | `_tree`, `_internal_method` |

### Best Practices (Do's and Don'ts)

| Category | [ + ] Do this | [ - ] Don't do this |
| :--- | :--- | :--- |
| **File Paths** | Use `os.path.join` or `pathlib` for compatibility. | Hardcode paths like `C:\Users` or `/home`. |
| **Debugging** | Use proper error handling and logging. | Leave `print()` statements in production code. |
| **Security** | Use environment variables/config files. | Commit API keys, passwords, or secrets. |
| **Arguments** | Use `None` as default for mutable args. | Use mutable defaults like `def f(x=[]):`. |

### Import Order
Imports should be grouped and separated by a blank line:

1.  **Standard Library** (`os`, `json`, `typing`)
2.  **Third-Party** (`lark`, `colorama`)
3.  **Local Application** (`configx.core.tree`)

---

## [ Contribution Workflow ]

### 1. Select an Issue
Browse the **Issues** tab.
> **Note:** Winter of Code participants should look for issues tagged `good first issue` or `woc-2025`.

### 2. Create a Branch
Create a new branch for your work using a descriptive name.

```bash
git checkout -b <type>/<description>
```

### 3. Make Changes & Test
Write your code and ensure it passes all tests.
```bash
pytest tests/
```

### 4. Commit Changes
We follow **Conventional Commits**. Please use the following prefixes:

| Prefix | Use Case |
| :--- | :--- |
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `style` | Formatting, missing semi-colons, etc. |
| `refactor` | Code change that neither fixes a bug nor adds a feature |

**Example:**
```bash
git commit -m "feat: add support for persistent snapshots"
```

### 5. Push & Pull Request
Push your branch and open a Pull Request (PR) on GitHub.
```bash
git push origin <your-branch-name>
```

---

## [ Reporting Bugs & Suggestions ]

| Type | Action |
| :--- | :--- |
| **Bug Report** | Open an Issue using the "Bug Report" template. Include steps to reproduce and environment details. |
| **Feature Request** | Open an Issue tagged `enhancement`. Describe the feature and its utility. |

---

## [ Getting Help ]

If you get stuck or have questions:

*  Comment on the specific Issue or Pull Request.
*  Reach out to the maintainers via email: `adityagaur.home@gmail.com`, 

**Happy Coding!**
