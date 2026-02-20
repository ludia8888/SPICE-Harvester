import { OverlayToaster, Position, type ToastProps } from '@blueprintjs/core'

type ToasterLike = {
  show: (props: ToastProps, key?: string) => string
}

let toasterPromise: Promise<ToasterLike> | null = null

const getToaster = () => {
  if (!toasterPromise) {
    toasterPromise = OverlayToaster.create({
      position: Position.TOP_RIGHT,
      maxToasts: 5,
    }) as Promise<ToasterLike>
  }
  return toasterPromise
}

export const showAppToast = async (props: ToastProps, key?: string) => {
  const toaster = await getToaster()
  toaster.show(props, key)
}
