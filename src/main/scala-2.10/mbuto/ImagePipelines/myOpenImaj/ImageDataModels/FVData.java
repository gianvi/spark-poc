package mbuto.ImagePipelines.myOpenImaj.ImageDataModels;


import org.openimaj.feature.DoubleFV;


public interface FVData extends ImageData {
    DoubleFV getFeatureVector();
}
