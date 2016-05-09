package mbuto.ImagePipelines.myOpenImaj.ImageDataModels;


import org.openimaj.image.FImage;
import org.openimaj.image.ImageProvider;
import org.openimaj.io.ReadWriteable;


public interface ImageData extends ReadWriteable, ImageProvider<FImage> {
    FImage getImage();

    String getType();

    String getPath();
}
