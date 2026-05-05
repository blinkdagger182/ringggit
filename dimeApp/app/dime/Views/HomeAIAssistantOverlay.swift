//
//  HomeAIAssistantOverlay.swift
//  dime
//

import SwiftUI
import UIKit

struct HomeAIAssistantOverlay: View {
    private let assistantTitle = "Renvo AI"

    @ObservedObject var viewModel: HomeAIAssistantViewModel
    @StateObject private var keyboardHeightHelper = KeyboardHeightHelper()

    let namespace: Namespace.ID
    let topInset: CGFloat
    let bottomInset: CGFloat
    let revealProgress: CGFloat
    let isExpanded: Bool
    let onCollapse: () -> Void
    let collapseGesture: AnyGesture<DragGesture.Value>?

    @State private var composerFocused = false
    @State private var composerFocusRequestID = 0
    @State private var chatScrollRequestID = 0
    @State private var showAttachmentSheet = false
    @State private var previewAttachment: AttachmentItem?

    var body: some View {
        GeometryReader { proxy in
            let keyboardOverlap = max(0, keyboardHeightHelper.keyboardHeight - proxy.safeAreaInsets.bottom)
            let baseComposerBottomPadding = max(proxy.safeAreaInsets.bottom, bottomInset) + 12
            let activeComposerBottomPadding = keyboardOverlap > 0 ? 12 : baseComposerBottomPadding

            ZStack(alignment: .bottom) {
                Color(.systemBackground)
                    .ignoresSafeArea()

                HomeAIAnimatedGradientBackground()
                    .opacity(max(0, revealProgress))
                    .ignoresSafeArea()

VStack(spacing: 0) {
                    previewHeader
                        .padding(.horizontal, 24)
                        .padding(.top, max(topInset, proxy.safeAreaInsets.top) + 10)
                        .opacity(revealProgress)

                    messageArea(
                        keyboardOverlap: keyboardOverlap,
                        composerBottomPadding: activeComposerBottomPadding
                    )
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                        .opacity(revealProgress)
                        .offset(y: (1 - revealProgress) * 24)
                }
                .frame(width: proxy.size.width, height: proxy.size.height, alignment: .top)

                composerSection
                    .padding(.bottom, activeComposerBottomPadding)
                    .opacity(revealProgress)
                    .offset(y: (1 - revealProgress) * 20 - keyboardOverlap)
                    .background(
                        LinearGradient(
                            colors: [
                                Color(.systemBackground).opacity(0),
                                Color(.systemBackground).opacity(0.92),
                                Color(.systemBackground)
                            ],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                    )
                    .zIndex(2)

                if isExpanded && keyboardOverlap == 0 {
                    collapseHandle
                        .padding(.bottom, activeComposerBottomPadding + 104)
                        .opacity(revealProgress)
                        .zIndex(1)
                        .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
                }
            }
            .ignoresSafeArea(.keyboard, edges: .bottom)
            .onChange(of: isExpanded) { expanded in
                guard expanded else { return }
                guard viewModel.messages.isEmpty else { return }

                DispatchQueue.main.asyncAfter(deadline: .now() + 0.38) {
                    guard isExpanded else { return }
                    focusComposer()
                }
            }
        }
        .fullScreenCover(item: $previewAttachment) { item in
            AttachmentPreviewView(item: item)
        }
        .sheet(isPresented: $showAttachmentSheet) {
            let sheet = HomeAIAttachmentSheet(
                attachments: $viewModel.pendingAttachments,
                onQuickPrompt: { prompt in
                    viewModel.draftMessage = prompt
                    viewModel.sendDraftMessage()
                }
            )
            if #available(iOS 16.0, *) {
                sheet
                    .presentationDetents([.medium])
                    .presentationDragIndicator(.hidden)
            } else {
                sheet
            }
        }
    }

    private var previewHeader: some View {
        HStack(spacing: 8) {
            Image(systemName: "sparkles")
                .font(.system(size: 14, weight: .semibold))
                .foregroundStyle(
                    LinearGradient(
                        colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                        startPoint: .topLeading, endPoint: .bottomTrailing
                    )
                )

            Text(assistantTitle)
                .font(.system(size: 20, weight: .semibold))
                .foregroundColor(Color(.label))

            Text("beta")
                .font(.system(size: 12, design: .rounded).weight(.medium))
                .foregroundColor(Color(.secondaryLabel))
                .padding(.horizontal, 9)
                .padding(.vertical, 4)
                .overlay(
                    Capsule(style: .continuous)
                        .stroke(Color(.separator), lineWidth: 1)
                )
        }
        .frame(maxWidth: .infinity)
    }

    private func messageArea(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> some View {
        ScrollViewReader { proxy in
            ScrollView(showsIndicators: false) {
                VStack(alignment: .leading, spacing: 16) {
                    if viewModel.messages.isEmpty {
                        emptyState
                    } else {
                        quickActionsSection(title: "Try asking")

                        ForEach(viewModel.messages) { message in
                            messageBubble(message)
                                .id(message.id)
                        }

                        if viewModel.isResponding {
                            typingIndicator
                                .id("typing-indicator")
                        }
                    }

                    nonBubbleDragArea(
                        height: bottomScrollSpacerHeight(
                            keyboardOverlap: keyboardOverlap,
                            composerBottomPadding: composerBottomPadding
                        )
                    )
                    .id("bottom-anchor")
                }
                .background(
                    Color.black.opacity(0.001)
                        .contentShape(Rectangle())
                        .onTapGesture {
                            dismissKeyboard()
                        }
                )
                .padding(.horizontal, 24)
                .padding(.top, 16)
            }
            .modifier(HomeAIInteractiveKeyboardDismissModifier())
            .background(
                nonBubbleDragArea(height: 1)
            )
            .onChange(of: viewModel.messages.count) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: viewModel.isResponding) { _ in
                requestScrollToChatEnd(proxy, animated: false)
            }
            .onChange(of: keyboardHeightHelper.keyboardHeight) { _ in
                requestScrollToChatEnd(proxy, animated: false, delay: 0.26)
            }
            .onChange(of: composerFocused) { focused in
                guard focused else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0.26)
            }
            .onAppear {
                guard !viewModel.messages.isEmpty else { return }
                requestScrollToChatEnd(proxy, animated: false, delay: 0)
            }
        }
    }

    private func nonBubbleDragArea(height: CGFloat) -> some View {
        Color.black.opacity(0.001)
            .frame(maxWidth: .infinity, minHeight: height, maxHeight: height)
            .contentShape(Rectangle())
            .onTapGesture {
                dismissKeyboard()
            }
            .modifier(HomeAICollapseGestureModifier(dragGesture: collapseGesture))
    }

    private var emptyState: some View {
        VStack(spacing: 20) {
            quickActionsSection(title: "Try asking")
        }
        .padding(.top, 8)
    }

    private func quickActionsSection(title: String) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            if !title.isEmpty {
                Text(title)
                    .font(.system(.subheadline).weight(.medium))
                    .foregroundColor(Color(.secondaryLabel))
                    .padding(.horizontal, 2)
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 12) {
                    ForEach(viewModel.quickActions) { quickAction in
                        Button {
                            viewModel.triggerQuickAction(quickAction)
                        } label: {
                            VStack(alignment: .leading, spacing: 10) {
                                ZStack {
                                    Circle()
                                        .fill(
                                            LinearGradient(
                                                colors: [Color(hex: "4ECDC4").opacity(0.18), Color(hex: "9BAAF8").opacity(0.22)],
                                                startPoint: .topLeading, endPoint: .bottomTrailing
                                            )
                                        )
                                        .frame(width: 34, height: 34)
                                    Image(systemName: quickAction.systemImage)
                                        .font(.system(size: 14, weight: .medium))
                                        .foregroundStyle(
                                            LinearGradient(
                                                colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                                startPoint: .topLeading, endPoint: .bottomTrailing
                                            )
                                        )
                                }

                                Spacer(minLength: 0)

                                Text(quickAction.title)
                                    .font(.system(.subheadline).weight(.semibold))
                                    .foregroundColor(Color(.label))
                                    .lineLimit(2)
                                    .multilineTextAlignment(.leading)

                                Text(quickActionSubtitle(for: quickAction.title))
                                    .font(.system(.caption, design: .rounded))
                                    .foregroundColor(Color(.secondaryLabel))
                                    .lineLimit(2)
                            }
                            .padding(14)
                            .frame(width: 148, height: 148, alignment: .leading)
                            .background(
                                RoundedRectangle(cornerRadius: 16, style: .continuous)
                                    .fill(Color(.secondarySystemBackground))
                                    .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.trailing, 24)
                .padding(.bottom, 4)
            }
            .scrollClipDisabledIfAvailable()
        }
    }

    private func messageBubble(_ message: HomeAIMessage) -> some View {
        HStack(alignment: .top) {
            if message.role == .assistant {
                VStack(alignment: .leading, spacing: 10) {
                    HStack(alignment: .top, spacing: 10) {
                        ZStack {
                            Circle()
                                .fill(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4").opacity(0.18), Color(hex: "9BAAF8").opacity(0.22)],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                                .frame(width: 30, height: 30)
                            Image(systemName: "sparkles")
                                .font(.system(size: 13, weight: .semibold))
                                .foregroundStyle(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                        }
                        .padding(.top, 2)

                        VStack(alignment: .leading, spacing: 5) {
                            Text("Renvo")
                                .font(.system(.subheadline).weight(.semibold))
                                .foregroundStyle(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "7B8FF8")],
                                        startPoint: .leading, endPoint: .trailing
                                    )
                                )

                            Text(message.text)
                                .font(.system(.body))
                                .foregroundColor(Color(.label))
                                .multilineTextAlignment(.leading)
                        }
                    }
                    .padding(16)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Color(.secondarySystemBackground))
                            .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
                    )

                    if let visual = message.visual {
                        AIVisualCardView(visual: visual)
                    }
                }

                Spacer(minLength: 42)
            } else {
                Spacer(minLength: 42)

                let hasText = !message.text.isEmpty && message.text != "Please extract all transactions from the attached image(s)."
                let firstAttachment = message.attachments.first

                VStack(alignment: .trailing, spacing: 0) {
                    if firstAttachment != nil || hasText {
                        VStack(alignment: .leading, spacing: 0) {
                            if let attachment = firstAttachment {
                                Button { previewAttachment = attachment } label: {
                                    Image(uiImage: attachment.thumbnail)
                                        .resizable()
                                        .scaledToFill()
                                        .frame(maxWidth: 220, maxHeight: 160)
                                        .clipped()
                                        .overlay(alignment: .bottomLeading) {
                                            if attachment.pages.count > 1 {
                                                Text(attachment.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                                                    .font(.system(size: 10, weight: .semibold, design: .rounded))
                                                    .foregroundColor(.white)
                                                    .padding(.horizontal, 6)
                                                    .padding(.vertical, 3)
                                                    .background(Color.black.opacity(0.55))
                                                    .clipShape(Capsule())
                                                    .padding(6)
                                            }
                                        }
                                }
                                .buttonStyle(.plain)
                            }

                            if hasText {
                                Text(message.text)
                                    .font(.system(.body).weight(.medium))
                                    .foregroundColor(.white)
                                    .padding(.horizontal, 14)
                                    .padding(.vertical, 11)
                            }
                        }
                        .background(
                            LinearGradient(
                                colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
                        .frame(maxWidth: 260, alignment: .trailing)
                    }
                }
            }
        }
    }

    private var typingIndicator: some View {
        HStack(alignment: .top, spacing: 10) {
            ZStack {
                Circle()
                    .fill(Color(hex: "EBF5FB"))
                    .frame(width: 30, height: 30)
                Image(systemName: "sparkles")
                    .font(.system(size: 13, weight: .semibold))
                    .foregroundColor(Color(hex: "3B82F6"))
            }
            .padding(.top, 2)

            HStack(spacing: 5) {
                Circle().fill(Color(.tertiaryLabel)).frame(width: 7, height: 7)
                Circle().fill(Color(.tertiaryLabel)).frame(width: 7, height: 7)
                Circle().fill(Color(.tertiaryLabel)).frame(width: 7, height: 7)
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 14)
            .background(
                RoundedRectangle(cornerRadius: 16, style: .continuous)
                    .fill(Color(.secondarySystemBackground))
                    .shadow(color: Color.black.opacity(0.06), radius: 8, y: 2)
            )

            Spacer(minLength: 42)
        }
    }

    private var composerSection: some View {
        HStack(alignment: .bottom, spacing: 10) {
            if keyboardHeightHelper.keyboardHeight > 0 {
                Button { showAttachmentSheet = true } label: {
                    ZStack {
                        Circle()
                            .fill(
                                LinearGradient(
                                    colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                    startPoint: .topLeading, endPoint: .bottomTrailing
                                )
                            )
                            .frame(width: 50, height: 50)
                        Image(systemName: "plus")
                            .font(.system(size: 20, weight: .medium))
                            .foregroundColor(.white)
                    }
                }
                .buttonStyle(.plain)
                .padding(.bottom, 3)
            }

            composer
        }
        .padding(.horizontal, 20)
    }

    private var composer: some View {
        VStack(spacing: 0) {
            // Attachment thumbnails inside the pill
            if !viewModel.pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(viewModel.pendingAttachments) { item in
                            composerAttachmentCell(item)
                        }
                    }
                    .padding(.horizontal, 14)
                    .padding(.vertical, 10)
                }
                .frame(height: 90)

                Rectangle()
                    .fill(Color(.separator).opacity(0.5))
                    .frame(height: 1)
                    .padding(.horizontal, 14)
            }

            // Input row
            HStack(alignment: .center, spacing: 12) {
                if keyboardHeightHelper.keyboardHeight == 0 {
                    Button { showAttachmentSheet = true } label: {
                        ZStack {
                            Circle()
                                .fill(
                                    LinearGradient(
                                        colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                        startPoint: .topLeading, endPoint: .bottomTrailing
                                    )
                                )
                                .frame(width: 32, height: 32)
                            Image(systemName: "plus")
                                .font(.system(size: 15, weight: .semibold))
                                .foregroundColor(.white)
                        }
                    }
                    .buttonStyle(.plain)
                }

                HomeAIComposerTextField(
                    text: $viewModel.draftMessage,
                    isFocused: composerFocusBinding,
                    focusRequestID: composerFocusRequestID,
                    placeholder: "Ask anything about your money...",
                    onSubmit: sendComposerMessage
                )
                .frame(maxWidth: .infinity, minHeight: 24, maxHeight: 24, alignment: .leading)

                if viewModel.hasContent {
                    Button { sendComposerMessage() } label: {
                        Image(systemName: "paperplane.fill")
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundStyle(
                                LinearGradient(
                                    colors: [Color(hex: "4ECDC4"), Color(hex: "9BAAF8")],
                                    startPoint: .topLeading, endPoint: .bottomTrailing
                                )
                            )
                            .frame(width: 32, height: 32)
                    }
                    .buttonStyle(.plain)
                }
            }
            .frame(height: 56)
            .padding(.horizontal, 16)
        }
        .contentShape(Rectangle())
        .onTapGesture { focusComposer() }
        .background(
            RoundedRectangle(cornerRadius: 20, style: .continuous)
                .fill(Color(.secondarySystemBackground))
                .overlay(
                    RoundedRectangle(cornerRadius: 20, style: .continuous)
                        .stroke(composerFocused ? Color(hex: "4ECDC4").opacity(0.4) : Color(.separator).opacity(0.5), lineWidth: 1)
                )
                .shadow(color: Color.black.opacity(composerFocused ? 0.08 : 0.04), radius: composerFocused ? 12 : 6, y: 2)
        )
        .simultaneousGesture(
            DragGesture(minimumDistance: 4)
                .onChanged { value in
                    guard value.translation.height > 8 else { return }
                    dismissKeyboard()
                }
        )
    }

    private func composerAttachmentCell(_ item: AttachmentItem) -> some View {
        ZStack(alignment: .topTrailing) {
            Button { previewAttachment = item } label: {
                ZStack(alignment: .bottomLeading) {
                    Image(uiImage: item.thumbnail)
                        .resizable()
                        .scaledToFill()
                        .frame(width: 68, height: 68)
                        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

                    if item.pages.count > 1 {
                        Text(item.label.components(separatedBy: "·").last?.trimmingCharacters(in: .whitespaces) ?? "")
                            .font(.system(size: 9, weight: .semibold, design: .rounded))
                            .foregroundColor(.white)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 2)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Capsule())
                            .padding(4)
                    }
                }
            }
            .buttonStyle(.plain)

            Button { viewModel.removeAttachment(id: item.id) } label: {
                ZStack {
                    Circle()
                        .fill(Color(hex: "2C2C2C"))
                        .frame(width: 20, height: 20)
                    Image(systemName: "xmark")
                        .font(.system(size: 9, weight: .bold))
                        .foregroundColor(.white)
                }
                .padding(3)
            }
            .buttonStyle(.plain)
        }
    }

    private var collapseHandle: some View {
        ZStack {
            Image(systemName: "chevron.up")
                .font(.system(size: 18, weight: .semibold))
                .foregroundColor(Color(.tertiaryLabel))
                .frame(width: 48, height: 28)
        }
        .frame(width: 112, height: 64)
        .contentShape(Rectangle())
        .accessibilityLabel("Swipe up to return Home")
    }

    private func requestScrollToChatEnd(_ proxy: ScrollViewProxy, animated: Bool, delay: TimeInterval = 0.04) {
        chatScrollRequestID += 1
        let requestID = chatScrollRequestID

        DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
            guard requestID == chatScrollRequestID else { return }

            let action = {
                proxy.scrollTo("bottom-anchor", anchor: .bottom)
            }

            if animated {
                withAnimation(.easeOut(duration: 0.2)) {
                    action()
                }
            } else {
                action()
            }
        }
    }

    private func sendComposerMessage() {
        guard !viewModel.draftMessage.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard !viewModel.isResponding else { return }

        focusComposer()
        viewModel.sendDraftMessage()
        focusComposer()

        DispatchQueue.main.async {
            focusComposer()
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
            focusComposer()
        }

        DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
            focusComposer()
        }
    }

    private func dismissKeyboard() {
        composerFocused = false
        UIApplication.shared.endEditing()
    }

    private func bottomScrollSpacerHeight(keyboardOverlap: CGFloat, composerBottomPadding: CGFloat) -> CGFloat {
        72 + composerBottomPadding + keyboardOverlap
    }

    private func focusComposer() {
        composerFocused = true
        composerFocusRequestID += 1
    }

    private func quickActionSubtitle(for title: String) -> String {
        switch title {
        case "Add transaction":
            return "Quickly log an expense"
        case "Where did my money go?":
            return "Breakdown by category"
        case "Show this month's spending":
            return "Review this month's flow"
        case "Can I afford this?":
            return "Check against current budget"
        default:
            return "Start with a suggested prompt"
        }
    }

    private var composerFocusBinding: Binding<Bool> {
        Binding(
            get: { composerFocused },
            set: { composerFocused = $0 }
        )
    }
}

private struct AttachmentPreviewView: View {
    let item: AttachmentItem
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Color.black.ignoresSafeArea()

            if item.pages.count == 1 {
                Image(uiImage: item.pages[0])
                    .resizable()
                    .scaledToFit()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                TabView {
                    ForEach(item.pages.indices, id: \.self) { i in
                        Image(uiImage: item.pages[i])
                            .resizable()
                            .scaledToFit()
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .tag(i)
                    }
                }
                .tabViewStyle(.page)
                .indexViewStyle(.page(backgroundDisplayMode: .always))
            }

            Button { dismiss() } label: {
                ZStack {
                    Circle()
                        .fill(Color.white.opacity(0.15))
                        .frame(width: 36, height: 36)
                    Image(systemName: "xmark")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.white)
                }
            }
            .buttonStyle(.plain)
            .padding(.top, 56)
            .padding(.trailing, 20)
        }
    }
}

private struct HomeAICollapseGestureModifier: ViewModifier {
    let dragGesture: AnyGesture<DragGesture.Value>?

    func body(content: Content) -> some View {
        if let dragGesture {
            content.highPriorityGesture(dragGesture)
        } else {
            content
        }
    }
}

private struct HomeAIInteractiveKeyboardDismissModifier: ViewModifier {
    func body(content: Content) -> some View {
        if #available(iOS 16.0, *) {
            content.scrollDismissesKeyboard(.interactively)
        } else {
            content
        }
    }
}

private extension View {
    @ViewBuilder
    func scrollClipDisabledIfAvailable() -> some View {
        if #available(iOS 17.0, *) {
            self.scrollClipDisabled()
        } else {
            self
        }
    }
}

private struct HomeAIComposerTextField: UIViewRepresentable {
    @Binding var text: String
    @Binding var isFocused: Bool

    let focusRequestID: Int
    let placeholder: String
    let onSubmit: () -> Void

    func makeUIView(context: Context) -> UITextField {
        let textField = UITextField(frame: .zero)
        textField.delegate = context.coordinator
        textField.backgroundColor = .clear
        textField.borderStyle = .none
        textField.returnKeyType = .send
        textField.textColor = UIColor.label
        textField.tintColor = UIColor(red: 0.306, green: 0.804, blue: 0.769, alpha: 1)
        textField.font = UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
        textField.autocorrectionType = .yes
        textField.autocapitalizationType = .sentences
        textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        textField.addTarget(context.coordinator, action: #selector(Coordinator.textDidChange(_:)), for: .editingChanged)
        updatePlaceholder(for: textField)
        return textField
    }

    func updateUIView(_ textField: UITextField, context: Context) {
        context.coordinator.parent = self

        if textField.text != text {
            textField.text = text
        }

        updatePlaceholder(for: textField)

        if context.coordinator.lastFocusRequestID != focusRequestID {
            context.coordinator.lastFocusRequestID = focusRequestID
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if isFocused && !textField.isFirstResponder {
            DispatchQueue.main.async {
                textField.becomeFirstResponder()
            }
        } else if !isFocused && textField.isFirstResponder {
            textField.resignFirstResponder()
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    private func updatePlaceholder(for textField: UITextField) {
        textField.attributedPlaceholder = NSAttributedString(
            string: placeholder,
            attributes: [
                .foregroundColor: UIColor.placeholderText,
                .font: UIFont.roundedSystemFont(ofSize: 17, weight: .regular)
            ]
        )
    }

    final class Coordinator: NSObject, UITextFieldDelegate {
        var parent: HomeAIComposerTextField
        var lastFocusRequestID: Int

        init(parent: HomeAIComposerTextField) {
            self.parent = parent
            self.lastFocusRequestID = parent.focusRequestID
        }

        @objc func textDidChange(_ textField: UITextField) {
            parent.text = textField.text ?? ""
        }

        func textFieldDidBeginEditing(_ textField: UITextField) {
            parent.isFocused = true
        }

        func textFieldDidEndEditing(_ textField: UITextField) {
            parent.isFocused = false
        }

        func textFieldShouldReturn(_ textField: UITextField) -> Bool {
            parent.onSubmit()
            return false
        }
    }
}

private extension UIFont {
    static func roundedSystemFont(ofSize size: CGFloat, weight: UIFont.Weight) -> UIFont {
        let systemFont = UIFont.systemFont(ofSize: size, weight: weight)
        guard let descriptor = systemFont.fontDescriptor.withDesign(.rounded) else {
            return systemFont
        }
        return UIFont(descriptor: descriptor, size: size)
    }
}

private struct HomeAIAnimatedGradientBackground: View {
    var body: some View {
        ZStack {
            RadialGradient(
                colors: [Color(hex: "4ECDC4").opacity(0.28), Color.clear],
                center: .topLeading,
                startRadius: 0,
                endRadius: 340
            )
            RadialGradient(
                colors: [Color(hex: "9BAAF8").opacity(0.22), Color.clear],
                center: .topTrailing,
                startRadius: 0,
                endRadius: 320
            )
        }
    }
}
