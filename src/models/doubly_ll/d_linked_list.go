package doublyll

import (
	"fmt"
)

type Block struct {
	data     []byte
	number   int
	filename string
	next     *Block
	prev     *Block
}

func NewNode(data []byte, number int, filename string) *Block {
	return &Block{
		data:     data,
		number:   number,
		filename: filename,
		next:     nil,
		prev:     nil,
	}
}

func (n *Block) GetData() []byte {
	return n.data
}

func (n *Block) SetData(data []byte) {
	n.data = data
}

func (n *Block) GetNumber() int {
	return n.number
}

func (n *Block) GetFilename() string {
	return n.filename
}

type DoublyLinkedList struct {
	head   *Block
	tail   *Block
	length int
}

func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{}
}

func (list *DoublyLinkedList) Front() *Block {
	return list.head
}

func (list *DoublyLinkedList) Back() *Block {
	return list.tail
}

func (list *DoublyLinkedList) Display() {

	if list.head == nil {
		fmt.Printf("No Data Present in Linked List.\n")
	} else {
		temp := list.head
		for temp != nil {
			fmt.Printf("%v -> ", temp.data)
			temp = temp.next
		}
		fmt.Println("END")
	}
}

func (list *DoublyLinkedList) ListLength() int {
	return list.length
}

func (list *DoublyLinkedList) InsertBeginning(n *Block) {

	if list.head == nil {
		list.head = n
		list.tail = n
		n.prev = nil
		n.next = nil
	} else {
		list.head.prev = n
		n.next = list.head
		list.head = n
	}
	list.length++
}

func (list *DoublyLinkedList) InsertEnd(n *Block) {

	if list.head == nil {
		list.InsertBeginning(n)
	} else {
		n.prev = list.tail
		list.tail.next = n
		list.tail = n
		list.length++
	}
}

func (list *DoublyLinkedList) InsertAtSpecificPosition(n *Block, pos int) {

	if pos >= list.length {
		fmt.Printf("Size Exceeding\n")
	} else {
		if pos == 0 {
			list.InsertBeginning(n)
		} else if pos == -1 {
			list.InsertEnd(n)
		} else {
			temp := list.head
			index := 0
			for index < pos-1 {
				temp = temp.next
				index++
			}
			// fmt.Println(temp.data)
			temp.next.prev = n
			n.next = temp.next
			n.prev = temp
			temp.next = n
			list.length++
		}
	}

}

func (list *DoublyLinkedList) DeleteFromBegining() {

	if list.head == nil {
		fmt.Printf("Empty Linked List\n")
	} else {
		if list.length == 1 {
			list.head = nil
			list.tail = nil
		} else {
			list.head = list.head.next
			list.head.prev = nil
		}
		list.length--
	}
}

func (list *DoublyLinkedList) DeleteFromEnd() {

	if list.head == nil {
		fmt.Printf("Empty Linked List\n")
	} else {
		if list.length == 1 {
			list.head = nil
			list.tail = nil
		} else {
			list.tail = list.tail.prev
			list.tail.next = nil
		}
		list.length--
	}

}

func (list *DoublyLinkedList) DeleteFromSpecificPosition(pos int) {

	if pos >= list.length {
		fmt.Printf("Size Exceeding\n")
	} else {
		if pos == 0 {
			list.DeleteFromBegining()
		} else if pos == -1 {
			list.DeleteFromEnd()
		} else {
			temp := list.head
			index := 0
			for index < pos {
				temp = temp.next
				index++
			}

			if temp == list.tail {
				list.DeleteFromEnd()
			} else {
				temp.next.prev = temp.prev
				temp.prev.next = temp.next
				list.length--
			}
		}
	}

}

func (list *DoublyLinkedList) DeleteLinkedList() {

	if list.head != nil {
		temp := list.head
		for temp.next != nil {
			temp.prev = nil
			temp = temp.next
		}
		list.head = nil
		list.length = 0
	}

}

func (list *DoublyLinkedList) ViewHeadTail() {
	fmt.Println(list.head.data)
	fmt.Println(list.tail.data)
	fmt.Println(list.tail.prev.data)
}

func (dll *DoublyLinkedList) MoveToFront(node *Block) {
	if node == dll.head {
		return
	}

	if node == dll.tail {
		dll.tail = node.prev
		dll.tail.next = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}

	node.next = dll.head
	node.prev = nil
	dll.head.prev = node
	dll.head = node
}
