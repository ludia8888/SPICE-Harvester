import { OverlayToaster, Position, type Toaster, type ToastProps } from '@blueprintjs/core'

let toasterPromise: Promise<Toaster> | null = null

const getToaster = () => {
  if (typeof document === 'undefined') {
    return null
  }

  if (!toasterPromise) {
    toasterPromise = OverlayToaster.create({
      position: Position.TOP_RIGHT,
      maxToasts: 5,
    })
  }

  return toasterPromise
}

export const showAppToast = async (props: ToastProps, key?: string) => {
  const promise = getToaster()
  if (!promise) {
    return null
  }

  const toaster = await promise
  return toaster.show(props, key)
}

