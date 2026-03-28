# ComfyUI-ZMongo

An implementation of a MongoDB interface in ComfyUI.

Since your local folder accidentally got synced to the main ComfyUI source code, a "scorched earth" approach is actually the cleanest way to get your specific toolset back.

Here are the full commands to delete the current directory and pull a brand-new copy from your private repository.

### The "Start Fresh" Commands

Run these in order on your Alienware terminal:

```bash
# 1. Move to the custom_nodes directory
cd /home/comfyuser/comfy_build/ComfyUI/custom_nodes
```
# 2. Delete the current (and incorrect) ZMongo folder
# WARNING: This is permanent, but since we are re-cloning, it's safe.
```bash 
rm -rf ComfyUI_ZMongo
```

# 3. Clone your private repository fresh
```bash
git clone https://github.com/CentralFloridaAttorney/ComfyUI_ZMongo.git
```
# 4. Enter the new folder
```bash
cd ComfyUI_ZMongo
```


### A Few Important Notes:

* **Credentials:** Because `ComfyUI_ZMongo` is a **private** repository, GitHub will likely ask for your username and password. 
    * **Password Tip:** You cannot use your account password; you must use a **Personal Access Token (PAT)**. If you have SSH keys set up on that Alienware, use `git clone git@github.com:CentralFloridaAttorney/ComfyUI_ZMongo.git` instead.
* **Verification:** Once the clone finishes, run `ls` to make sure you see your `zmongo_manager.py` and the `zmongo_toolbag` folder.
* **Dependencies:** Since this is a fresh start, don't forget to make sure your requirements are installed in your virtual environment:
    ```bash
    pip install -r requirements.txt
    ```

**Would you like me to generate a quick bash script you can save on the Alienware to automate this "fresh pull" if things ever get messy again?**