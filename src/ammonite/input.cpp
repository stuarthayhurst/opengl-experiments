#include <iostream>

#include "internal/inputManager.hpp"
#include "internal/internalControls.hpp"
#include "enums.hpp"
#include "types.hpp"

#define OVERRIDE_MODE_DEFAULT AMMONITE_FORCE_RELEASE

namespace ammonite {
  namespace input {
    //A: Multi-key, override mode, no toggle
    //Hands off to core
    int registerKeybind(int keycodes[], int count, AmmoniteEnum overrideMode,
                        AmmoniteKeyCallback callback, void* userPtr) {
      return internal::registerRawKeybind(keycodes, count, overrideMode, false, callback, userPtr);
    }

    //B: Multi-key, override mode, toggle
    //Hands off to core
    int registerToggleKeybind(int keycodes[], int count, AmmoniteEnum overrideMode,
                              AmmoniteKeyCallback callback, void* userPtr) {
      return internal::registerRawKeybind(keycodes, count, overrideMode, true, callback, userPtr);
    }

    //C: Multi-key, no override mode, no toggle
    //Hands off to A
    int registerKeybind(int keycodes[], int count,
                        AmmoniteKeyCallback callback, void* userPtr) {
      return registerKeybind(keycodes, count, OVERRIDE_MODE_DEFAULT, callback, userPtr);
    }

    //D: Multi-key, no override mode, toggle
    //Hands off to B
    int registerToggleKeybind(int keycodes[], int count,
                              AmmoniteKeyCallback callback, void* userPtr) {
      return registerToggleKeybind(keycodes, count, OVERRIDE_MODE_DEFAULT, callback, userPtr);
    }

    //E: Single key, override mode, no toggle
    //Hands off to C
    int registerKeybind(int keycode, AmmoniteEnum overrideMode,
                        AmmoniteKeyCallback callback, void* userPtr) {
      return registerKeybind(&keycode, 1, overrideMode, callback, userPtr);
    }

    //F: Single key, override mode, toggle
    //Hands off to D
    int registerToggleKeybind(int keycode, AmmoniteEnum overrideMode,
                              AmmoniteKeyCallback callback, void* userPtr) {
      return registerToggleKeybind(&keycode, 1, overrideMode, callback, userPtr);
    }

    //G: Single key, no override mode, no toggle
    //Hands off to E
    int registerKeybind(int keycode, AmmoniteKeyCallback callback, void* userPtr) {
      return registerKeybind(keycode, OVERRIDE_MODE_DEFAULT, callback, userPtr);
    }

    //H: Single key, no override mode, toggle
    //Hands off to F
    int registerToggleKeybind(int keycode, AmmoniteKeyCallback callback, void* userPtr) {
      return registerToggleKeybind(keycode, OVERRIDE_MODE_DEFAULT, callback, userPtr);
    }

    int unregisterKeybind(int keybindId) {
      return internal::unregisterKeybind(keybindId);
    }

    //Return true if all keys are at least part of the same combo
    bool isKeycodeRegistered(int keycodes[], int count) {
      return internal::isKeycodeRegistered(keycodes, count);
    }

    //Single key variant of the above
    bool isKeycodeRegistered(int keycode) {
      return isKeycodeRegistered(&keycode, 1);
    }

    void setInputFocus(bool active) {
      internal::setInputBlock(!active);
      ammonite::controls::internal::setCursorFocus(active);
    }

    bool getInputFocus() {
      return !internal::getInputBlock();
    }

    int changeKeybind(int keybindId, int keycodes[], int count) {
      return internal::changeKeybindKeycodes(keybindId, keycodes, count);
    }

    int changeKeybind(int keybindId, int keycode) {
      return internal::changeKeybindKeycodes(keybindId, &keycode, 1);
    }

    void updateInput() {
        //Poll GLFW for input, run keybind callbacks
        glfwPollEvents();
        ammonite::input::internal::runCallbacks();
    }
  }
}
