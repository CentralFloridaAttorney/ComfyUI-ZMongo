import os
import torch
import numpy as np
from PIL import Image, ImageOps
import folder_paths


class ZMongoGoogleDriveLoader:
    @classmethod
    def INPUT_TYPES(s):
        input_dir = folder_paths.get_input_directory()
        files = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
        return {
            "required": {
                "image": (sorted(files), {"image_upload": True}),
            }
        }

    CATEGORY = "ZMongo/Drive"
    RETURN_TYPES = ("IMAGE", "MASK")
    FUNCTION = "load_image"

    def load_image(self, image):
        image_path = folder_paths.get_annotated_filepath(image)

        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)

        if img.mode == 'I':
            img = img.point(lambda i: i * (1 / 255))
        image_rgb = img.convert("RGB")

        image_tensor = torch.from_numpy(np.array(image_rgb).astype(np.float32) / 255.0)[None,]

        # Create a blank mask as a safe default
        mask = torch.zeros((1, 64, 64), dtype=torch.float32, device="cpu")

        return (image_tensor, mask)


NODE_CLASS_MAPPINGS = {
    "ZMongoGoogleDriveLoader": ZMongoGoogleDriveLoader
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoGoogleDriveLoader": "ZMongo Google Drive Loader"
}