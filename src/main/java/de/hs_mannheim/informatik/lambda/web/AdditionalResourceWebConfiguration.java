package de.hs_mannheim.informatik.lambda.web;

import de.hs_mannheim.informatik.lambda.controller.LambdaController;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.File;

@Configuration
public class AdditionalResourceWebConfiguration implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        File nonNormalizedTCPath = new File(LambdaController.NON_NORMALIZED_CLOUD_PATH);
        File normalizedTCPath = new File(LambdaController.NORMALIZED_CLOUD_PATH);

        if (!nonNormalizedTCPath.exists())
            nonNormalizedTCPath.mkdir();

        if (!normalizedTCPath.exists())
            normalizedTCPath.mkdir();

        registry.addResourceHandler("/" + LambdaController.NON_NORMALIZED_CLOUD_PATH + "**").addResourceLocations("file:" + LambdaController.NON_NORMALIZED_CLOUD_PATH);
        registry.addResourceHandler("/" + LambdaController.NORMALIZED_CLOUD_PATH + "**").addResourceLocations("file:" + LambdaController.NORMALIZED_CLOUD_PATH);
    }

}