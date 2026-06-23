use super::*;

#[test]
#[expect(clippy::unwrap_used)]
fn test_growing_windows() {
    let a = [1, 2, 3, 4, 5];

    let mut windows = a.growing_windows();

    assert_eq!(&[1], windows.next().unwrap());
    assert_eq!(&[2], windows.next().unwrap());
    assert_eq!(&[3], windows.next().unwrap());
    assert_eq!(&[4], windows.next().unwrap());
    assert_eq!(&[5], windows.next().unwrap());

    assert_eq!(&[1, 2], windows.next().unwrap());
    assert_eq!(&[2, 3], windows.next().unwrap());
    assert_eq!(&[3, 4], windows.next().unwrap());
    assert_eq!(&[4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2, 3], windows.next().unwrap());
    assert_eq!(&[2, 3, 4], windows.next().unwrap());
    assert_eq!(&[3, 4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2, 3, 4], windows.next().unwrap());
    assert_eq!(&[2, 3, 4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2, 3, 4, 5], windows.next().unwrap());
}

#[test]
#[expect(clippy::unwrap_used)]
fn test_shrinking_windows() {
    let a = [1, 2, 3, 4, 5];

    let mut windows = a.shrinking_windows();

    assert_eq!(&[1, 2, 3, 4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2, 3, 4], windows.next().unwrap());
    assert_eq!(&[2, 3, 4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2, 3], windows.next().unwrap());
    assert_eq!(&[2, 3, 4], windows.next().unwrap());
    assert_eq!(&[3, 4, 5], windows.next().unwrap());

    assert_eq!(&[1, 2], windows.next().unwrap());
    assert_eq!(&[2, 3], windows.next().unwrap());
    assert_eq!(&[3, 4], windows.next().unwrap());
    assert_eq!(&[4, 5], windows.next().unwrap());

    assert_eq!(&[1], windows.next().unwrap());
    assert_eq!(&[2], windows.next().unwrap());
    assert_eq!(&[3], windows.next().unwrap());
    assert_eq!(&[4], windows.next().unwrap());
    assert_eq!(&[5], windows.next().unwrap());
}
