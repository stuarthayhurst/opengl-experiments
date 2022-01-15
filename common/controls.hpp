#ifndef CONTROLS
#define CONTROLS

namespace controls {
  namespace matrix {
    glm::mat4 getViewMatrix();
    glm::mat4 getProjectionMatrix();
  }

  namespace settings {
    void setMovementSpeed(float newMovementSpeed);
    void setMouseSpeed(float newMouseSpeed);
    void setZoomSpeed(float newZoomMultiplier);

    float getMovementSpeed();
    float getMouseSpeed();
    float getZoomSpeed();
  }

  void setupControls(GLFWwindow* newWindow, int* widthAddr, int* heightAddr, float* aspectRatioAddr);
  void processInput();
}

#endif
